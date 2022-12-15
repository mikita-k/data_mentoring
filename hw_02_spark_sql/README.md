---
* Add your code in `src/main/` if needed
* Test your code with `src/tests/` if needed
* Modify notebooks for your needs
* Deploy infrastructure with terraform
```
terraform init
terraform plan -out terraform.plan
terraform apply terraform.plan
....
terraform destroy
```
* Launch notebooks on Databricks cluster

1. register in databricks + MSAzure
2. open databricks azure
3. create Workspace
4. launch Workspace
5. create a cluster
---
