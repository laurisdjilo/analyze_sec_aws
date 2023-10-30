import aws_cdk as cdk
from constructs import Construct
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_s3_deployment as s3_deployment
from aws_cdk import aws_iam as iam
from aws_cdk import aws_glue as glue

SEC_FS_DATASET_SOURCE_URL = "https://www.sec.gov/files/dera/data/financial-statement-data-sets/"

class AnalyzeSecAwsStack(cdk.Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        self.__create_bucket()
        self.__upload_assets()
        self.__create_glue_db()
        self.__create_job_role()
        self.__create_jobs()
        self.__build_workflows()
    
    def __create_glue_db(self):
        self.glue_database = glue.CfnDatabase(
            self,
            "Glue_Database",
            catalog_id=self.account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name=self.node.get_context("glue_db_name")
            )
        )
    
    def __create_jobs(self):
        
        self.catchup_collect_job = self.__create_collect_job(
            job_name="catchup_collect_job",
            script_name="catchup_collect_sec_fs_datasets.py",
            description="This job is made to catchup on SEC Financial Statements since Q1 2009",
            max_capacity=1
        )

        self.collect_sec_fs_dataset = self.__create_collect_job(
            job_name="collect_sec_fs_dataset",
            script_name="due_collect_sec_fs_datasets.py",
        )

        self.fs_sub_transform_job = self.__create_transform_job(
            job_name="fs_sub_transform_job",
            table_name="fs_sub",
            write_mode="append",
            sub_folder="FS_SUB",
            cluster_cols="adsh",
            cluster_table="true",
            num_clusters="3",
            partition="true"
        )

        self.fs_num_transform_job = self.__create_transform_job(
            job_name="fs_num_transform_job",
            table_name="fs_num",
            write_mode="append",
            sub_folder="FS_NUM",
            cluster_cols="adsh",
            cluster_table="true",
            num_clusters="3",
            partition="true"
        )

        self.fs_pre_transform_job = self.__create_transform_job(
            job_name="fs_pre_transform_job",
            table_name="fs_pre",
            write_mode="append",
            sub_folder="FS_PRE",
            cluster_cols="adsh",
            cluster_table="true",
            num_clusters="3",
            partition="true"
        )

        self.fs_tag_transform_job = self.__create_transform_job(
            job_name="fs_tag_transform_job",
            table_name="fs_tag",
            write_mode="overwrite",
            sub_folder="FS_TAG",
            cluster_cols="adsh",
            cluster_table="false",
            num_clusters="3",
            partition="false"
        )

    def __build_workflows(self):
        # Creation of the scheduled workflow
        self.sec_fs_dataset_pipeline = glue.CfnWorkflow(
            self,
            "sec_fs_dataset_pipeline",
            name="sec_fs_dataset_pipeline",
            description="""
            This pipeline is made to collect SEC Financial Statements and transform the data.
            It's schedule to run a few days after the end of each quarter
            """,
        )

        initial_trigger = glue.CfnTrigger(
            self,
            "initial_trigger",
            type="SCHEDULED",
            actions=[
                glue.CfnTrigger.ActionProperty(
                    job_name=self.collect_sec_fs_dataset.name
                )
            ],
            workflow_name=self.sec_fs_dataset_pipeline.name,
            schedule="cron(0 0 L JAN,APR,JUL,OCT ? *)"
        )
        
        transfrom_trigger = glue.CfnTrigger(
            self,
            "transform_trigger",
            type="CONDITIONAL",
            actions=[
                glue.CfnTrigger.ActionProperty(
                    job_name=self.fs_num_transform_job.name
                ),
                glue.CfnTrigger.ActionProperty(
                    job_name=self.fs_pre_transform_job.name
                ),
                glue.CfnTrigger.ActionProperty(
                    job_name=self.fs_sub_transform_job.name
                ),
                glue.CfnTrigger.ActionProperty(
                    job_name=self.fs_tag_transform_job.name
                )
            ],
            workflow_name=self.sec_fs_dataset_pipeline.name,
            predicate=glue.CfnTrigger.PredicateProperty(
                logical="AND",
                conditions=[
                    glue.CfnTrigger.ConditionProperty(
                        logical_operator="EQUALS",
                        job_name=self.collect_sec_fs_dataset.name,
                        state="SUCCEEDED"
                    )
                ]
            )
        )

        # Creation of the catchup workflow
        self.catchup_sec_fs_dataset_pipeline = glue.CfnWorkflow(
            self,
            "catchup_sec_fs_dataset_pipeline",
            name="catchup_sec_fs_dataset_pipeline",
            description="""
            This pipeline is made to catchup on SEC Financial Statements and transform the data.
            """
        )

        catchup_initial_trigger = glue.CfnTrigger(
            self,
            "catchup_initial_trigger",
            type="ON_DEMAND",
            actions=[
                glue.CfnTrigger.ActionProperty(
                    job_name=self.catchup_collect_job.name
                )
            ],
            workflow_name=self.catchup_sec_fs_dataset_pipeline.name
        )
        
        catchup_transfrom_trigger = glue.CfnTrigger(
            self,
            "catchup_transfrom_trigger",
            type="CONDITIONAL",
            actions=[
                glue.CfnTrigger.ActionProperty(
                    job_name=self.fs_num_transform_job.name
                ),
                glue.CfnTrigger.ActionProperty(
                    job_name=self.fs_pre_transform_job.name
                ),
                glue.CfnTrigger.ActionProperty(
                    job_name=self.fs_sub_transform_job.name
                ),
                glue.CfnTrigger.ActionProperty(
                    job_name=self.fs_tag_transform_job.name
                )
            ],
            workflow_name=self.catchup_sec_fs_dataset_pipeline.name,
            predicate=glue.CfnTrigger.PredicateProperty(
                logical="AND",
                conditions=[
                    glue.CfnTrigger.ConditionProperty(
                        logical_operator="EQUALS",
                        job_name=self.catchup_collect_job.name,
                        state="SUCCEEDED"
                    )
                ]
            )
        )
    
    def __create_collect_job(self, job_name, script_name, description="", max_capacity=0.0625):
        return glue.CfnJob(
            self,
            job_name,
            name=job_name,
            description=description,
            max_capacity=max_capacity,
            glue_version="3.0",
            role=self.job_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="pythonshell",
                python_version="3.9",
                script_location= "s3://{}/sources/job_scripts/{}".format(self.s3_bucket.bucket_name, script_name)
            ),
            default_arguments={
                "--s3_bucket": self.s3_bucket.bucket_name,
                "--s3_prefix_dest": "DATA/RAW/",
                "--source_url": SEC_FS_DATASET_SOURCE_URL,
                "--job-language": "python",
                "--TempDir": "s3://{}/temporary/".format(self.s3_bucket.bucket_name),
                "--extra-py-files": "s3://{}/sources/libs/sec_fs_dataset_collector-0.1-py3-none-any.whl".format(self.s3_bucket.bucket_name),
            }
        )

    def __create_transform_job(self, job_name, table_name, sub_folder, write_mode, cluster_cols="", cluster_table="", num_clusters="", partition="", worker_type="G.1X", number_of_workers=5):
        return glue.CfnJob(
            self,
            job_name,
            name=job_name,
            glue_version="4.0",
            role=self.job_role.role_arn,
            worker_type=worker_type,
            number_of_workers=number_of_workers,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location= "s3://{}/sources/job_scripts/{}".format(self.s3_bucket.bucket_name, "transform_sec_fs_dataset.py")
            ),
            default_arguments={
                "--s3_bucket": self.s3_bucket.bucket_name,
                "--archive_s3_prefix": "DATA/ARCHIVE/{}/".format(sub_folder),
                "--cluster_cols": cluster_cols,
                "--cluster_table": cluster_table,
                "--database": self.node.get_context("glue_db_name"),
                "--num_clusters": num_clusters,
                "--partition_by_file_year": partition,
                "--raw_s3_prefix": "DATA/RAW/{}/".format(sub_folder),
                "--table_name": table_name,
                "--table_s3a_location": "s3a://{}/DATA/TRANSFORM/{}/".format(self.s3_bucket.bucket_name, sub_folder),
                "--write_mode": write_mode,
                "--job-language": "python",
                "--job-bookmark-option": "job-bookmark-disable",
                "--TempDir": "s3://{}/temporary/".format(self.s3_bucket.bucket_name),
                "--enable-metrics": "true",
                "--enable-continuous-cloudwatch-log": "true",
                "--enable-spark-ui": "true",
                "--spark-event-logs-path": "s3://{}/sparkHistoryLogs/".format(self.s3_bucket.bucket_name),
                "--enable-glue-datacatalog": "true",
                "--enable-auto-scaling": "true",
                "--enable-job-insights": "false"
            }
        )

    def __create_bucket(self):
        self.s3_bucket = s3.Bucket(
            self,
            "stack_bucket",
            bucket_name=self.node.get_context("bucket_name"),
            versioned=True,
            auto_delete_objects=True,
            removal_policy=cdk.RemovalPolicy.DESTROY
        )
        # add lifecyle rule to aws cdk bucket construct. The rule transitions objects from given prefix to glacier deep archive after 0 days
        self.s3_bucket.add_lifecycle_rule(
            prefix="DATA/ARCHIVE",
            transitions=[
                s3.Transition(
                    storage_class=s3.StorageClass.GLACIER,
                    transition_after=cdk.Duration.days(0)
                )
            ]
        )

    def __upload_assets(self):
        """
        Deploy the resources under the folder ./assets as assets under the prefix sources of the bucket s3_bucket
        """
        s3_deployment.BucketDeployment(
            self,
            "Scripts_Upload",
            sources=[
                s3_deployment.Source.asset("./assets/job_scripts")
            ],
            destination_bucket=self.s3_bucket,
            destination_key_prefix="sources/job_scripts"
        )
        s3_deployment.BucketDeployment(
            self,
            "Libs_Upload",
            sources=[
                s3_deployment.Source.asset("./assets/libs/sec_fs_dataset_collector/dist/")
            ],
            destination_bucket=self.s3_bucket,
            destination_key_prefix="sources/libs"
        )

    def __create_job_role(self):
        if self.node.try_get_context("job_role_arn"):
            self.job_role = iam.Role.from_role_arn(self, "job_role", self.node.try_get_context("job_role_arn"))
        else:
            """
            Create an IAM role, attach to that role the policy service-role/AWSGlueServiceRole, and grant that role the permission to get and put objects in the bucket self.s3_bucket
            """
            self.job_role = iam.Role(
                self,
                "job_role",
                assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
                managed_policies=[iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole")],
            )
        self.job_role.add_to_policy(
            iam.PolicyStatement(
                actions=["s3:GetObject", "s3:PutObject"],
                resources=[self.s3_bucket.bucket_arn + "/*"]
            )
        )
