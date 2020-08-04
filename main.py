from data_processing import *
import argparse


if __name__ == '__main__':

    parser = argparse.ArgumentParser()

    parser.add_argument("--path_", type=str,  help="Dictionary for data")
    parser.add_argument("--format_name", type=str,  help="name of format")
    parser.add_argument("--table_name", type=str,  help="name of table")
    parser.add_argument("--stage_name", type=str,  help="name of stage")


    file_list = [file for file in os.listdir(args.path_) if '.json' in str(file)]
    file_list.sort()

    print(file_list)

    cs = db_cursor()


    create_table(args.table_name)
    create_format(args.format_name)
    create_stage(args.stage_name, args.format_name)

    for _, file_name in enumerate(file_list):

        # pickle to json:
        file_path = args.path_ + file_name
        # load from local to table
        stage_file(args.stage_name, args.file_path)
        list_stage(args.stage_name)
        check_table(args.table_name)

    copy_files(args.table_name, args.stage_name, args.format_name, file_list)
    remove_stage(args.stage_name)

    db_close(cs)
