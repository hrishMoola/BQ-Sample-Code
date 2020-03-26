# CS686 Lab 07

Up-to-date [README file](https://github.com/cs-rocks/cs686-lectures/blob/master/labs/Lab07-README.md)

## About some app bundles in data ##

This is completely irrelevant to the lab assignment, but in case you are wondering what the bundles really mean:
 - [`app.짜장면`](https://en.wikipedia.org/wiki/Jajangmyeon)
 - [`app.냉면`](https://en.wikipedia.org/wiki/Naengmyeon)
 - [`app.라면`](https://en.wikipedia.org/wiki/Instant_noodle#South_Korea)
 - [`app.비빔면`](https://en.wikipedia.org/wiki/Paldo_bibim_men)
 - `커피` = coffee
 - `피자` = pizza
 - `치킨` = chicken

In my defense, it was 2 am when I was making up the datasets...


Hello! This is one of my latest projects in BigQuery. The readme above outlines the main tasks to be done.

The starter code provided had only the comments above the functions, and local tests.

In order to run this code :

Clone the repo
Build the project on IntelliJ from the java/dataflow directory. (All development activity is over here!)
execute "gradle cleanTest test -- continue"
This will generate the protofiles if needed, and run all the sample tests.

The code:

1. All the required queries are in java/dataflow/resources. The tasks are in the readme above.
2. The datasets in questions are in the DF-Code repo. These have to loaded into BQ first as a table first. All text is JSON and delimited by "\n". 
3. Data is from GCP's load balancers(simulated). A certain url field has to be parsed for more relevant information. This was done using a JS UDF. 
4. The queries are divided according to tasks mentioned. The tasks are the same as the one in DF project.
5. Enjoy!   
