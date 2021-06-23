import os
import csv
import pandas
import plotly.graph_objs as go
from plotly.subplots import make_subplots
from src.database_handler import DatabaseHandler
from src.processing import process_file, build_graph, build_dataframe

database_path = os.path.normpath(os.path.pardir + os.path.join('/dummy_database/dummy.db'))
figure_path = os.path.normpath(os.path.pardir + os.path.join('/dummy_database/dummy_figure.html'))
table_name = "invoices_dummy"
