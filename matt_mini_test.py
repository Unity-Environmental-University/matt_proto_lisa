import json
import pandas as pd
import os
import sys
import asyncio
from matt_test_dap import download_table_from_dap
from matt_count_json import collect_2025_records
import difflib  # for closest name matching
import numpy as np

# TODO also needs to be added to git and set up with a gitignore
# TODO can make it technically fully functional but bad by pulling DAP data & creating csvs in this file
# might want to add stuff like course_name, student_name, instructor_name, grade_time columns, etc
# can add like a "loading..." while it downloads

# TODO you have to be cd'd into the right directory
# and because this is outside main it happens even if cmdline arg is bad
# users_df = pd.read_csv("users_2025.csv")
# enrollments_df = pd.read_csv("enrollments.csv", dtype={15: "string"}, parse_dates=[16]) # force column 14 to string and 15 to date
# submissions_df = pd.read_csv("submissions_2025.csv", na_values=["<unset>"], dtype={38: "string", 39: "string"})    # TODO <unset> row is strange, seems user id was cut off
# terms_df = pd.read_csv("enrollment_terms_2025.csv")
# courses_df = pd.read_csv("course_sections_2025.csv")

## function that lets you search user table for name - returns list of ids that match that name
def gather_user_id_from_name(name, users_df):
    name_column = users_df["name"]
    closest = difflib.get_close_matches(name, name_column.tolist(), n=1)[0]  # get closest matching name

    ## grab first result (should only be one) of looking for closest term name in the id column
    matching_name_id = users_df.loc[users_df["name"] == closest, "id"].iloc[0]

    return matching_name_id   # do I want to do it this way or just filer the df and return that?

## takes a user id and returns all courses they are in, gathered from enrollments table
def gather_course_ids_matching_user_id_from_enrollments(user_id, enrollments_df):   # make sure theyre a teacher
    mask_user = enrollments_df["user_id"] == user_id
    mask_teacher = enrollments_df["type"] == "TeacherEnrollment"
    combined_mask = mask_user & mask_teacher
    matching_courses = enrollments_df.loc[combined_mask, "course_id"]

    return matching_courses.tolist()

# takes a course_id and grabs all submissions for it from the submissions table
# TODO I want this to return a df filtered down to just those relevant submissions
def grab_submissions_for_course(course_id, submissions_df):
    filtered_submissions = submissions_df[submissions_df["course_id"] == course_id].copy()

    return filtered_submissions
    # TODO theres going to be a lot of courses, combine all filtered dfs into one big one? They're all submissions so should be easy - can do that in main()
    # TODO need to make resulting csv have instructor + student name, maybe a grade delta column(that uses cached_due_date with a warning if there was no submitted_at)
# TODO maybe add a function to average all grade deltas in a dataframe

## search enrollment_terms using given term_name and return the id(s) for it
def gather_term_ids_from_term_name(term_name, term_df):
    term_name_column = term_df["name"]
    closest = difflib.get_close_matches(term_name, term_name_column.tolist(), n=1)[0] # get closest matching name

    ## grab first result (should only be one) of looking for closest term name in the id column
    matching_term_id = term_df.loc[term_df["name"] == closest, "id"].iloc[0]

    # TODO do the same for instructor name above
    # TODO add error handling - what if closest doesn't find a match?
    return matching_term_id

def filter_courses_by_term(enrollment_term_id, course_ids, courses_df):
    filtered_courses_df = pd.DataFrame()
    for i in range(len(course_ids)):
        filter_i = courses_df[courses_df["id"] == course_ids[i]].copy()
        filtered_courses_df = pd.concat([filtered_courses_df, filter_i], ignore_index=True)
    print((filtered_courses_df))

    filtered_courses_etids = filtered_courses_df[courses_df["enrollment_term_id"] == enrollment_term_id].copy()
    new_courselist = filtered_courses_etids["id"].tolist()
    print(len(new_courselist))

    # course_ids_np = np.array(course_ids, dtype=np.int64)
    # filtered_courses = courses_df.query(
    #     "enrollment_term_id == @enrollment_term_id and id in @course_ids_np"
    # )["id"].tolist()    # TODO either i grabbing the wrong stuff or theres really nothing there, but I think theres something there TODO also i think i should make really small dfs so I can actually see and test my stuff
    # filtered_courses = courses_df[
    #     (courses_df["enrollment_term_id"] == enrollment_term_id) &
    #     (courses_df["id"].isin(course_ids))
    #     ]["id"].tolist()
    # print(type(courses_df["id"][0]))
    # print(type(courses_df["enrollment_term_id"][0]))
    # print(type(enrollment_term_id))
    # print(type(course_ids_np[0]))

    return new_courselist


# users = gather_user_ids_from_name("Richard Gray") # TODO assuming this has 1 entry, might want to force it to
# print(users)
# their_courses = gather_course_ids_matching_user_id_from_enrollments(12791306)
# print(their_courses)
# their_assoc_submissions = grab_submissions_for_course(their_courses[0])
# print(their_assoc_submissions.to_string())
# print(len(submissions_df))

async def main(): # TODO make this async since Ill be calling an async function
    # if len(sys.argv) != 2:
    #     print("""Usage: python3 matt_mini_test.py 'Instructor Name'""")
    #     sys.exit(1)     # exit if teacher name not supplied
    #
    # instructor_name = sys.argv[1]

    download_folder = "table_downloads"
    csv_folder = "table_2025_csvs"

    users_folder = os.path.join(download_folder, "users") # TODO i use table names a lot, should I make strings?
    enrollments_folder = os.path.join(download_folder, "enrollments")
    enrollment_terms_folder = os.path.join(download_folder, "enrollment_terms")
    submissions_folder = os.path.join(download_folder, "submissions")
    courses_folder = os.path.join(download_folder, "courses")

    users_csv = os.path.join(csv_folder, "users.csv")
    enrollments_csv = os.path.join(csv_folder, "enrollments.csv")
    enrollment_terms_csv = os.path.join(csv_folder, "enrollment_terms.csv")
    submissions_csv = os.path.join(csv_folder, "submissions.csv")
    courses_csv = os.path.join(csv_folder, "courses.csv")

    os.makedirs(download_folder, exist_ok=True)
    os.makedirs(csv_folder, exist_ok=True)
    # download DAP data (function in matt_test_dap)
    # TODO 1 at a time, ie download users, then process it to csv, then do submissions & process, etc - ONLY if I decide to delete & grab new snapshot each time
    if not os.path.exists(users_folder):   # only download if the data isn't there - directory is made inside dl func
        await download_table_from_dap("users", users_folder)  # TODO pickup here!!!!!!! need to modify matt_count_json next to accomodate the extra folder level and delete things after. Also check DAP snapshots to see if we can avoid downloading everything? Also, you'll need the space on disk anyway? But taking a new snapshot will keep it updated! lol or dont delete and practice the incremental update from docs
    if not os.path.exists(enrollments_folder):
        await download_table_from_dap("enrollments", enrollments_folder)
    if not os.path.exists(enrollment_terms_folder):
        await download_table_from_dap("enrollment_terms", enrollment_terms_folder)
    if not os.path.exists(submissions_folder):
        await download_table_from_dap("submissions", submissions_folder)
    if not os.path.exists(courses_folder):
        await download_table_from_dap("courses", courses_folder)
    # TODO make sure its downloaded? Doesnt jobs alredy do that
    # TODO repeated if statements are dumb and I should probably change it - also, what if out of space or smthn? - add error handling

    if not os.path.exists(submissions_csv): # only make csvs if they're not already there
        collect_2025_records(submissions_folder, "created_at", submissions_csv)
    if not os.path.exists(users_csv):
        collect_2025_records(users_folder, "updated_at", users_csv)
    if not os.path.exists(enrollments_csv):
        collect_2025_records(enrollments_folder, "updated_at", enrollments_csv)
    if not os.path.exists(enrollment_terms_csv):
        collect_2025_records(enrollment_terms_folder, "updated_at", enrollment_terms_csv)
    if not os.path.exists(courses_csv): # TODO add ability to skip fields
        collect_2025_records(courses_folder, "updated_at", courses_csv, ["syllabus_body"])
    # TODO what can I do with incremental update from DAP? Can that update a job
    # TODO I already have?

    ## either way, we need to load our csv's as dfs
    users_df = pd.read_csv(users_csv)
    enrollments_df = pd.read_csv(enrollments_csv, dtype={15: "string"}, parse_dates=[16]) # force column 14 to string and 15 to date
    submissions_df = pd.read_csv(submissions_csv, dtype={38: "string", 39: "string"})    # TODO <unset> row is strange, seems user id was cut off - might just be csv display weirdness
    terms_df = pd.read_csv(enrollment_terms_csv)
    courses_df = pd.read_csv(courses_csv)

    #instructor_name = "Catherine White"
   # term_name = "DE5W05.19.25"      # TODO add option to not give term name?


    instructor_name = "Kennady Brinley"
    term_name = "DE5W02.24.25" # TODO Print id to check - should be 17895



    # TODO grab term id
    id_matching_name = gather_user_id_from_name(instructor_name, users_df)
    id_matching_term_name = gather_term_ids_from_term_name(term_name, terms_df)
    if not id_matching_name:
        print("no matching instructors found")
        sys.exit(1)
    #
    instructor_id = id_matching_name # TODO assuming only one for now, update

    courses_they_teach = gather_course_ids_matching_user_id_from_enrollments(instructor_id, enrollments_df)
    if not courses_they_teach:
        print("no matching courses found")
        sys.exit(1)

    # courses_they_teach is not a dataframe, its a list of ids
    courses_they_teach_from_term = filter_courses_by_term(id_matching_name, courses_they_teach, courses_df)

    submissions_in_courses = pd.DataFrame()
    #
    ## merge the dfs for the other courses they teach together
    for i in range(0, len(courses_they_teach_from_term)):
        submissions_in_i = grab_submissions_for_course(courses_they_teach[i], submissions_df) # this is a df
        submissions_in_courses = pd.concat([submissions_in_courses, submissions_in_i], ignore_index=True)

    if submissions_in_courses.empty:
        print("no matching courses found")
    else:
        result_folder = "resulting_csv"
        os.makedirs(result_folder, exist_ok=True)
        result = os.path.join(result_folder, "lisa_test_report.csv")
        submissions_in_courses.to_csv(result, index=False)
        print(submissions_in_courses)



if __name__ == "__main__":
    asyncio.run(main())
