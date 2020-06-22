
import re

def clear_comma(df, col):

    df[col] = df[col].apply(lambda x: re.sub(',', '.', str(x)))
    return df

def pickle_to_json(path):

    file = open(path, 'rb')

    # dump information to that file
    data = pickle.load(file)
    data_json = json.dumps(data)

    json_path = path.replace(".pickle", ".json")
    with open(json_path, 'w') as outfile:
        json.dump(data_json, outfile, sort_keys=True, indent=4)
