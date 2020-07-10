from data_processing import *


source = '.../OPENDATA_BOOKING_CALL_A_BIKE.csv'
target = '.../bike_test.csv'



format_name = 'xxx'
table_name = 'xxx'
stage_name = 'xxx'


file_list = [file for file in os.listdir(path_) if '.json' in str(file)]
file_list.sort()


create_table(table_name)
create_format(format_name)
create_stage(stage_name, format_name)
