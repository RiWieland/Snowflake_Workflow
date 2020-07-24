from data_processing import *


source = '.../OPENDATA_BOOKING_CALL_A_BIKE.csv'
target = '.../bike_test.csv'



format_name = 'xxx'
table_name = 'xxx'
stage_name = 'xxx'


file_list = [file for file in os.listdir(path_) if '.json' in str(file)]
file_list.sort()

print(file_list)

cs = db_cursor()


create_table(table_name)
create_format(format_name)
create_stage(stage_name, format_name)

for _, file_name in enumerate(file_list):

    # pickle to json:
    file_path = path_ + file_name
    # load from local to table
    stage_file(stage_name, file_path)
    list_stage(stage_name)
    check_table(table_name)

copy_files(table_name, stage_name, format_name, file_list)
remove_stage(stage_name)

db_close(cs)
