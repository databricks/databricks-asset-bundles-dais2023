# Databricks Asset Bundles

_Write code once, deploy to lakehouses everywhere..._

<img src='./data/bundle.jpeg' width=400>

_Click [here](https://www.youtube.com/watch?v=9HOgYVo-WTM) to watch the talk on Databricks Asset Bundles at Data & AI Summit 2023._

_The slides from the presentation are available [here](https://docs.google.com/presentation/d/1bnnTR19j_nZhB0bDCMoGga-8Sq6eBjhBAom-6NJ6F0I/edit?usp=sharing)._

## Introduction
_Databricks Asset Bundles_, also known simply as bundles, enable you to programmatically validate, deploy, and run the projects you are working on in Databricks via the [Databricks CLI](https://github.com/databricks/cli).  A bundle is a collection of one or more related files that contain:

* Any local artifacts (such as source code) to deploy to a remote Databricks workspace prior to running any related Databricks workflows.

* The declarations and settings for the Databricks jobs, Delta Live Tables pipelines, or [MLOps Stacks](https://github.com/databricks/mlops-stack) that act upon the artifacts that were deployed into the workspace.

For more information on bundles, please see the following pages in Databricks documentation:

#### Tutorials
* [Bundle development tasks](https://docs.databricks.com/dev-tools/bundles/work-tasks.html)
* [How to use Bundles with Databricks Workflows (aka Jobs)](https://docs.databricks.com/workflows/jobs/how-to/use-bundles-with-jobs.html)
* [Automate a DLT pipeline with DABs](https://docs.databricks.com/delta-live-tables/tutorial-bundles.html)
* [Run A CI/CD process with DABs and GitHub Actions](https://docs.databricks.com/dev-tools/bundles/ci-cd.html)

#### Reference 
* [bundle settings reference](https://docs.databricks.com/dev-tools/bundles/settings.html)
* [bundle command group reference](https://docs.databricks.com/dev-tools/cli/bundle-commands.html)

## Analyzing Databricks Medium Posts from Field Engineering
In this repo you'll find a simple project consisting of:

1. A CSV containing URLs of Medium.com blogs written by Field Engineers at Databricks.
2. A Delta Live Tables pipeline to ingest and process that data, including logic to scrape Medium.com for the number of claps and reading time.
3. A notebook report that reads the processed data and visualizes it.
4. A Databricks Workflow with two tasks - the first to refresh the DLT pipeline and the second to execute the notebook report.

These data assets are represented in the `bundle.yml` file in the project root directory.  

#### Deploying and running this repo
Make sure you have the Databricks CLI installed, then you can use the `databricks bundle` commands.  You'll also want to edit the `bundle.yml` and specify the Databricks Workspace that you plan to deploy to.  Once you've got that sorted out, you can deploy and run the project using the following commands:

```
databricks bundle deploy
databricks bundle run fe_medium_metrics
```

## Questions?
Please email dabs-preview@databricks.com if you have questions on DABs or if you have questions on the code in this repo, please email rafi.kurlansik@databricks.com
