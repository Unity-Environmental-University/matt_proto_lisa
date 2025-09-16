import json
import pandas as pd
import os

## TODO table_downloads -> users, submissions, enrollments, etc
## within users -> job blah -> part 0, part 1, etc
## makes csv of 2025 records for json in given folder
def collect_2025_records(folder_path, year_column, output_csv="records_2025.csv", ignored_fields=None):
    records_in_2025 = []
    total_lines = 0
    recency_count = 0

    jobs_folder = os.listdir(folder_path)[0] # within users folder will be "job-blah" - within that is parts
    jobs_folder = os.path.join(folder_path, jobs_folder)
    # Iterate over all .json files in the folder # TODO i need SOME primary keys, those are ["key] - need users, - why not just add all? probably best from data standpoint
    for filename in os.listdir(jobs_folder):
        if filename.endswith(".json"):
            file_path = os.path.join(jobs_folder, filename)
            with open(file_path, "r", encoding="utf-8") as f:
                for line in f:
                    if line.strip():  # skip empty lines
                        total_lines += 1
                        data = json.loads(line)
                        if data["value"][year_column][:4] == "2025":
                            relevant_data = data["value"].copy()

                            ## ignore specified fields
                            for field in ignored_fields:
                                relevant_data.pop(field, None)  # return none if key not found

                            ## add id column from key field to end
                            relevant_data["id"] = data["key"]["id"] # rearrange to front when this is pandas df
                            recency_count += 1
                            records_in_2025.append(relevant_data)
            # TODO delete the file after it has been loaded - it will soon be a csv

    # Convert to DataFrame and export
    if records_in_2025:
        df_2025 = pd.DataFrame(records_in_2025)
        id_col = df_2025.pop("id")
        df_2025.insert(0, "id", id_col)
        df_2025.to_csv(output_csv, index=False)
        print(f"saved {len(df_2025)} entries from 2025 to {output_csv}")
    else:
        print("No 2025 records")

    print(f"Total lines: {total_lines}")
    print(f"Total 2025 entries: {recency_count}")

#file = open("part-00000-34acd901-ffed-4f14-9574-eca1a001aa33-c000.json", "rb")
# def countlines(filename):
#     count = 0
#     recency_count = 0
#     records_in_2025 = []
#     with open(f"job_e05d790d-009b-40b9-baef-dcb2accdd852/{filename}", "r", encoding="utf-8") as f:
#         #data = [json.loads(line) for line in f]
#         #return sum(1 for line in f if line.strip())
#         for line in f:
#             if line.strip():
#                 count += 1
#                 data = json.loads(line)
#                 if data["value"]["created_at"][:4] == "2025":   # TODO create pandas df of 2025 entries? just to test. Export as csv for re-use
#                     recency_count += 1
#                     records_in_2025.append(data["value"])
#
#     if records_in_2025:
#         df_2025 = pd.DataFrame(records_in_2025)
#         df_2025.to_csv(f"{filename}.csv", index=False)
#
#         print(len(df_2025))
#     return count

# names = [
#     "part-00000-7b20a9b2-d130-4007-baca-988b606cd49e-c000.json",
#     "part-00001-7b20a9b2-d130-4007-baca-988b606cd49e-c000.json",
#     "part-00002-7b20a9b2-d130-4007-baca-988b606cd49e-c000.json",
#     "part-00003-7b20a9b2-d130-4007-baca-988b606cd49e-c000.json",
#     "part-00004-7b20a9b2-d130-4007-baca-988b606cd49e-c000.json",
#     "part-00005-7b20a9b2-d130-4007-baca-988b606cd49e-c000.json"
# ]

# count_total = sum(countlines(n) for n in names)
# print(count_total)

#print(countlines(names[2])) # TODO this will serve as csv test for submissions - small 100k entries - might want more

#collect_2025_records("submissions_test", "created_at", "submissions_2025.csv")
# collect_2025_records("matt_proto_lisa/users", "updated_at", "users_2025.csv")
# collect_2025_records("matt_proto_lisa/course_sections", "updated_at", "course_sections_2025.csv")
# collect_2025_records("matt_proto_lisa/enrollment_terms", "updated_at", "enrollment_terms_2025.csv")
# collect_2025_records("matt_proto_lisa/enrollments", "updated_at", "enrollments.csv")

## part 0 has 0 2025 submissions (for submissions stuff - the only folder unnamed)
## part 1 has 0
## part 2 has 116,764
## part 3 has 0
## part 4 has 999,574
## part 5 has 0

## in total there are 7,080,967 submissions (dates back to like 2013 or earlier)