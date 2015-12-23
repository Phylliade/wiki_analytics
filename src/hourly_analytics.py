import pandas as pd
import requests
import datetime
import os.path

# Please indicate a valid int
timezone_delta = 2
resource_folder = "../resources"
resource_files = [resource_folder + "/" + resource_file for resource_file in ["20150101", "20150201", "20150301"]]
blacklist_path = resource_folder + "/" + "blacklist_domains_and_pages"

blacklist = pd.read_table(blacklist_path, sep=' ', names=["domain", "title"])


def get_table_from_resource(file_path):
    print("Importing the resource into memory")
    return pd.read_table(file_path, sep=' ', names=["domain", "title", "count", "response_time"], usecols=["domain", "title", "count"])


def group_df(dataframe):
    return dataframe.groupby(["domain"])


def get_resource_name(target_date):
    return target_date.strftime("pagecounts-%Y%m%d-%H0000.gz")


def get_resource(target_date):
    # Get the path of the resource file
    resource_path = resource_folder + "/" + get_resource_name(target_date)

    if not os.path.isfile(resource_path):
        # Download only if necessary
        print("Downloading the resource file for the date : " + str(target_date))

        print("http://dumps.wikimedia.org/other/pagecounts-all-sites/" + target_date.strftime("%Y/%Y-%m/pagecounts-%Y%m%d-%H0000.gz"))
        with open(resource_path, "wb") as resource_fd:
            resource_data = requests.get("http://dumps.wikimedia.org/other/pagecounts-all-sites/" + target_date.strftime("%Y/%Y-%m/pagecounts-%Y%m%d-%H0000.gz"), stream=True)
            for chunk in resource_data.iter_content(2048):
                resource_fd.write(chunk)
    else:
        print("Resource file already present")


def hourly_ranking(dates=None):
    if dates is None:
        # If the date is not given, take the last available data
        dates = [datetime.datetime.now() - datetime.timedelta(hours=2 + timezone_delta)]

    for date in dates:
        floor_date = datetime.datetime(date.year, date.month, date.day, date.hour, 0)
        output_path = "../output/ranking" + str(floor_date) + ".csv"
        if os.path.isfile(output_path):
            print("Ranking for this date already present in {}, exiting.".format(floor_date))
        else:
            print("Computing the ranking")
            get_resource(floor_date)

            table = get_table_from_resource(resource_folder + "/" + get_resource_name(floor_date))

            blacklist_mask = table[["domain", "title"]].isin(blacklist.to_dict(outtype='list')).all(axis=1)
            table = table[~blacklist_mask]

            table_groups = group_df(table)
            del(table)

            with open(output_path, 'a') as output_fd:
                for name, group in table_groups:
                    group.sort(("count"), ascending=False, inplace=True)
                    output_fd.write(name + "\n---\n")
                    group[:25].to_csv(output_fd)
                    output_fd.write("----------\n\n")


if __name__ == "__main__":
    hourly_ranking()
