## Purpose
This project is a PoC of an automated AWS architecture intended to query the Security Exchange Comission (SEC) financial statement datasets using SQL.

## Architecture
![architecture.png](architecture\architecture.drawio.png)

## Deployment

1) Install the AWS CLI [[doc](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)] and configure your AWS credentials [[doc](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html)]
2) Install Node.js version `14.15.0` or `later`
3) Install the AWS CDK Toolkit globally using the following Node Package Manager command.
    ```
    npm install -g aws-cdk
    ```
    Then run the following command to verify correct installation and print the version number of the AWS CDK.
    ```
    cdk --version
    ```
4) Setup the required resources in your AWS account using the command below. Add the parameter `--profile` in case you wan to use profile different from the default
    ```
    cdk bootstrap aws://ACCOUNT-NUMBER/REGION
    ```
5) Run the command below being at the root of the repository. Don't forget to add the parameter `--region` (mentioning the region from step 4). And eventually the parameter `--profile` if necessary. This will provision your AWS account with the resources corresponding to the architecture above.
    ```
    cdk deploy -c glue_db_name="DATABASE-NAME" -c bucket_name="BUCKET-NAME"
    ```
    Note that a Glue database with the name you'll give will be created, and an S3 bucket with the name you mentioned will be created as well.
6) Run the commands below to catchup on the SEC financial statement datasets
    ```
    aws glue start-workflow-run --name catchup_sec_fs_dataset_pipeline
    ```
At this point you should be able to query the transformed SEC datasets using Athena.

**`Next step, we intend to build QuickSight dashboards upon these data.`**

In case you want to get rid of the architecture, just run the command below. Just make sure to provide the same parameters you did at the step 5).
```
cdk destroy -c glue_db_name="DATABASE-NAME" -c bucket_name="BUCKET-NAME"
```

## Useful links

- SEC Financial Datasets presentation : https://www.sec.gov/dera/data/financial-statement-data-sets

- SEC Financial Datasets description : https://www.sec.gov/files/aqfs.pdf

- SEC's list of forms : https://www.sec.gov/forms

- SEC's EDGAR tool to search for submissions : https://www.sec.gov/edgar/search/#

- How to read an 8-K form : https://www.sec.gov/oiea/investor-alerts-and-bulletins/how-read-8-k

## Tips

- To find a submission's document, just follow the link "https://www.sec.gov/Archives/edgar/data/{}/000003799622000056/f-20220819.htm".format(cik, adsh.replacd("-", ""), instance.replace(".xml", "").replace("_", "."))