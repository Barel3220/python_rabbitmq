# Python RabbitMQ Program Implementation
## 3 Modules System

## About

### First Module
First module sends a signal to the first queue, with a list of [path, type, table_name].

### Second Module
Second module is listening to the first queue forever, waiting for input.
Once arrived, loading the files and processing them into the database,
then sends a signal to the second queue with a table_name.

### Third Module
Third and last module is listeing to the second queue forever, waiting for input.
Once arrived, downloading the data from the database, extracting the relevant information
then creates a graph and displaying it.

This program was created by Barel Hatuka

## Usage

### Executing the Modules
**IMPORTANT NOTE:** This program is written in python 3.8.x, please make sure to install requirements from `requirements.txt`.
Moreover, this program require RabbitMQ to be installed, you can find a guide [HERE](https://www.rabbitmq.com/download.html)

There is no a correct execution order, each one of the modules can be ran first or last.
I like to run it from end to start:
- **Third Module** - *graph_consumer.py*
- **Second Module** - *database_consumer.py*
- **First Module** - *main.py*

`main.py` takes two variables: `(float, list)`, seconds in float, and a list of paths as listed above [path, type, table_name].
should look like: `main(amount, path_list)`

*already assigned in `if __name__` block and ready for execution*

### At the Execution
after you ran the *second module*, *third module* and finally the *first module*
you should have a local .html file open on your browser of choice for each row in the list you passed to main.


### After Execution
**NOTE:** *If you want to run the First Module again and again (Second and Third are running forever),
you should uncomment the cleanup() function and it's call from main in main.py*

*In case of not uncommenting the cleanup() function, the database will remain full,
which means that a second run will produce the end results for each row in the list you passed to main.*
