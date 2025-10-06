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
    mask_active = enrollments_df["workflow_state"] == "active" # TODO check w hallie about this - we only want the course the teacher is active in ya?
    combined_mask = mask_user & mask_teacher & mask_active
    matching_courses = enrollments_df.loc[combined_mask, "course_id"]


    return matching_courses.tolist()

# takes a course_id and grabs all submissions for it from the submissions table
# TODO I want this to return a df filtered down to just those relevant submissions
def grab_submissions_for_course(course_id, submissions_df):
    filtered_submissions = submissions_df[submissions_df["course_id"] == course_id]
    # TODO add masks to grab only submission


    return filtered_submissions
    # TODO theres going to be a lot of courses, combine all filtered dfs into one big one? They're all submissions so should be easy - can do that in main()
    # TODO need to make resulting csv have instructor + student name, maybe a grade delta column(that uses cached_due_date with a warning if there was no submitted_at)
# TODO maybe add a function to average all grade deltas in a dataframe

# takes a list of course ids and returns inactive users in those courses (a list)
def get_inactive_users(course_id_list, enrollments_df):
    mask_student = enrollments_df["type"] == "StudentEnrollment"
    mask_active = enrollments_df["workflow_state"] == "inactive"
    combined_mask = mask_student & mask_active

    inactive_users = []

    for i in range(len(course_id_list)):
        course_mask = enrollments_df["course_id"] == course_id_list[i]
        matching_users = enrollments_df.loc[combined_mask & course_mask, "user_id"].tolist()
        inactive_users.extend(matching_users)

    return inactive_users

## takes a list of users and removes them from the df its given
## modifies df in place
def remove_inactive_users(inactive_user_list, df):
    df.drop(df[df["user_id"].isin(inactive_user_list)].index, inplace=True)

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
        filter_i = courses_df[courses_df["id"] == course_ids[i]]
        filtered_courses_df = pd.concat([filtered_courses_df, filter_i], ignore_index=True)
    # print("here is courses dataframe filtered only by courses they teach")
    # print(filtered_courses_df)

    filtered_courses_etids = filtered_courses_df[filtered_courses_df["enrollment_term_id"] == enrollment_term_id]
    new_courselist = filtered_courses_etids["id"].tolist()
   # print("here is fancy new courselist", new_courselist)
    #print(len(new_courselist))

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

def add_course_names_to_submissions(submissions_df, courses_df):
    """Add course names to submissions dataframe by merging on course_id"""
    # Create a mapping of course_id to course name
    course_names = courses_df[['id', 'name']].rename(columns={'name': 'course_name'})

    # Merge submissions with course names
    submissions_with_names = submissions_df.merge(course_names, left_on='course_id', right_on='id', how='left', suffixes=('', '_course'))

    # Drop the duplicate id column from the merge
    submissions_with_names = submissions_with_names.drop('id_course', axis=1)

    return submissions_with_names

def add_instructor_names_to_submissions(submissions_df, enrollments_df, users_df):
    """Add instructor names to submissions dataframe by finding the teacher for each course"""
    # Get teacher enrollments only
    teacher_enrollments = enrollments_df[
        (enrollments_df['type'] == 'TeacherEnrollment') & 
        (enrollments_df['workflow_state'] == 'active')
    ][['course_id', 'user_id']].rename(columns={'user_id': 'instructor_id'})
    
    # Get instructor names
    instructor_names = users_df[['id', 'name']].rename(columns={'name': 'instructor_name', 'id': 'instructor_id'})
    
    # First merge to get instructor IDs for each course
    submissions_with_instructor_ids = submissions_df.merge(teacher_enrollments, on='course_id', how='left')
    
    # Then merge to get instructor names
    submissions_with_instructor_names = submissions_with_instructor_ids.merge(instructor_names, on='instructor_id', how='left')
    
    # Drop the instructor_id column as we only need the name
    submissions_with_instructor_names = submissions_with_instructor_names.drop('instructor_id', axis=1)
    
    return submissions_with_instructor_names

def add_student_names_to_submissions(submissions_df, users_df):
    """Add student names to submissions dataframe by merging on user_id"""
    # Create a mapping of user_id to student name
    student_names = users_df[['id', 'name']].rename(columns={'name': 'student_name'})
    
    # Merge submissions with student names
    submissions_with_student_names = submissions_df.merge(student_names, left_on='user_id', right_on='id', how='left', suffixes=('', '_student'))
    
    # Drop the duplicate id column from the merge
    submissions_with_student_names = submissions_with_student_names.drop('id_student', axis=1)
    
    return submissions_with_student_names

def add_term_names_to_submissions(submissions_df, courses_df, terms_df):
    """Add term names to submissions dataframe by linking through courses to enrollment terms"""
    # Get course to term mapping
    course_terms = courses_df[['id', 'enrollment_term_id']].rename(columns={'id': 'course_id'})
    
    # Get term names
    term_names = terms_df[['id', 'name']].rename(columns={'name': 'term_name', 'id': 'enrollment_term_id'})
    
    # First merge to get term IDs for each submission
    submissions_with_term_ids = submissions_df.merge(course_terms, on='course_id', how='left')
    
    # Then merge to get term names
    submissions_with_term_names = submissions_with_term_ids.merge(term_names, on='enrollment_term_id', how='left')
    
    # Drop the enrollment_term_id column as we only need the name
    submissions_with_term_names = submissions_with_term_names.drop('enrollment_term_id', axis=1)
    
    return submissions_with_term_names

def add_assignment_names_to_submissions(submissions_df, assignments_df):
    """Add assignment names to submissions dataframe by merging on assignment_id"""
    # Create a mapping of assignment_id to assignment name
    assignment_names = assignments_df[['id', 'title']].rename(columns={'title': 'assignment_name'})
    
    # Merge submissions with assignment names
    submissions_with_assignment_names = submissions_df.merge(assignment_names, left_on='assignment_id', right_on='id', how='left', suffixes=('', '_assignment'))
    
    # Drop the duplicate id column from the merge
    submissions_with_assignment_names = submissions_with_assignment_names.drop('id_assignment', axis=1)
    
    return submissions_with_assignment_names

def add_week_column_to_submissions(submissions_df):
    """
    Add week column to submissions dataframe by inferring week number from assignment_name.
    Looks for patterns like 'Week 1', 'Week 2', 'W1', 'W2', etc. in the assignment title.
    """
    import re
    
    df = submissions_df.copy()
    
    def extract_week(assignment_name):
        if pd.isna(assignment_name) or assignment_name is None:
            return None
        
        assignment_str = str(assignment_name).lower()
        
        # Look for various week patterns
        patterns = [
            r'week\s*(\d+)',        # "week 1", "week1", "Week 2", etc.
            r'w\s*(\d+)',           # "w1", "w 2", "W3", etc.
            r'wk\s*(\d+)',          # "wk1", "wk 2", "Wk3", etc.
            r'(\d+)\s*week',        # "1 week", "2week", etc. (reverse order)
        ]
        
        for pattern in patterns:
            match = re.search(pattern, assignment_str)
            if match:
                week_num = int(match.group(1))
                return f"Week {week_num}"
        
        return None
    
    # Apply the extraction function to create the week column
    df['week'] = df['assignment_name'].apply(extract_week)
    
    return df

def add_grade_delta_to_submissions(submissions_df):
    """
    Add grade_delta column to submissions dataframe.
    Calculates the difference between graded_at and cached_due_date.
    If graded_at is null, grade_delta will also be null.
    
    Args:
        submissions_df: DataFrame with cached_due_date and graded_at columns
    
    Returns:
        DataFrame with added grade_delta column (in days)
    """
    df = submissions_df.copy()
    
    # Convert date columns to datetime if they aren't already
    df['cached_due_date'] = pd.to_datetime(df['cached_due_date'], errors='coerce')
    df['graded_at'] = pd.to_datetime(df['graded_at'], errors='coerce')
    
    # Calculate grade delta: graded_at - cached_due_date
    # This will be positive if graded after due date, negative if graded before
    df['due_date_vs_graded_date(days)'] = (df['graded_at'] - df['cached_due_date']).dt.days
    
    return df

def add_compliance_status_to_submissions(submissions_df):
    """
    Add out_of_compliance column to submissions dataframe.
    True if graded_at is 72 hours or more after cached_due_date, False otherwise.
    Null if graded_at is null.
    
    Args:
        submissions_df: DataFrame with cached_due_date and graded_at columns
    
    Returns:
        DataFrame with added out_of_compliance column
    """
    df = submissions_df.copy()
    
    # Ensure date columns are datetime (should already be done by add_grade_delta_to_submissions)
    df['cached_due_date'] = pd.to_datetime(df['cached_due_date'], errors='coerce')
    df['graded_at'] = pd.to_datetime(df['graded_at'], errors='coerce')
    
    # Calculate if out of compliance (72+ hours = 3+ days after due date)
    # Will be NaN/null if graded_at is null
    df['out_of_compliance'] = (df['graded_at'] - df['cached_due_date']).dt.total_seconds() >= (72 * 3600)
    
    return df

def reorder_columns_for_readability(submissions_df):
    """
    Move the human-readable columns to the front of the dataframe for better readability
    """
    df = submissions_df.copy()
    
    # Rename id column to submission_id for clarity
    if 'id' in df.columns:
        df = df.rename(columns={'id': 'submission_id'})
    
    # Define the columns we want at the front (in order)
    front_columns = [
        'submission_id',
        'week',
        'assignment_name',
        'assignment_id',
        'student_name',
        'user_id',
        'instructor_name',
        'grader_id',
        'course_name',
        'course_id',
        'term_name',
        'workflow_state',
        'submitted_at',
        'cached_due_date',
        'graded_at',
        'due_date_vs_graded_date(days)',
        'out_of_compliance'
    ]
    
    # Get all other columns that aren't in our front list
    other_columns = [col for col in df.columns if col not in front_columns]
    
    # Only include front columns that actually exist in the dataframe
    existing_front_columns = [col for col in front_columns if col in df.columns]
    
    # Reorder: front columns first, then everything else
    new_column_order = existing_front_columns + other_columns
    
    return df[new_column_order]

def create_dean_friendly_dataframe(submissions_df):
    """
    Create a clean dataframe suitable for dean/administrative review by dropping
    technical columns and keeping only essential information
    """
    df = submissions_df.copy()
    
    # Columns to drop - technical IDs and detailed Canvas metadata
    # Note: keeping submission_id (renamed from id) and removing other technical IDs
    columns_to_drop = [ # grader ID
        'body',  # submission body text
        'url',  # submission URL
        'turnitin_data',  # turnitin details
        'quiz_submission_id',  # quiz submission ID
        'submission_comments_count',  # comment count
        'grade_matches_current_submission',  # technical flag
        'posted_at',  # posting timestamp
        'grading_period_id',  # grading period ID
        'preview_url',  # preview URL
        'group_id',
        'media_object_id',
        'media_comment_type',
        'attachment_id',
        'lti_user_id',
        'processed',
        'attachment_ids',
        'cached_quiz_lti',
        'anonymous_id',
    ]
    
    # Only drop columns that actually exist in the dataframe
    columns_to_drop = [col for col in columns_to_drop if col in df.columns]
    
    # Drop the unnecessary columns
    dean_df = df.drop(columns=columns_to_drop)
    
    return dean_df

async def main(): # TODO make this async since Ill be calling an async function
    if len(sys.argv) != 3:
        print("""Usage: python3 matt_mini_test.py 'Instructor Name' 'TermName' """)
        sys.exit(1)     # exit if teacher name not supplied
    # TODO first thing we do is check to see if we need new snapshot and delete table_downloads if we do
    # can just put last_date_of_access in an access_log file/folder whatever
    ## test stuff
    # instructor_name = "Catherine White"
    # # term_name = "DE5W05.19.25"      # TODO add option to not give term name? also course 4195124 should have enrollment id 17895
    #
    # # instructor_name = "Kennady Brinley"
    # term_name = "DE5W02.24.25"  # TODO Print id to check - should be 17895
    ## test stuff

    instructor_name = sys.argv[1]
    term_name = sys.argv[2]

    download_folder = "table_downloads"
    csv_folder = "table_2025_csvs"

    users_folder = os.path.join(download_folder, "users") # TODO i use table names a lot, should I make strings?
    enrollments_folder = os.path.join(download_folder, "enrollments")
    enrollment_terms_folder = os.path.join(download_folder, "enrollment_terms")
    submissions_folder = os.path.join(download_folder, "submissions")
    courses_folder = os.path.join(download_folder, "courses")
    assignments_folder = os.path.join(download_folder, "assignments")

    users_csv = os.path.join(csv_folder, "users.csv")
    enrollments_csv = os.path.join(csv_folder, "enrollments.csv")
    enrollment_terms_csv = os.path.join(csv_folder, "enrollment_terms.csv")
    submissions_csv = os.path.join(csv_folder, "submissions.csv")
    courses_csv = os.path.join(csv_folder, "courses.csv")
    assignments_csv = os.path.join(csv_folder, "assignments.csv")

    os.makedirs(download_folder, exist_ok=True)
    os.makedirs(csv_folder, exist_ok=True)
    # download DAP data (function in matt_test_dap)
    if not os.path.exists(users_folder):   # only download if the data isn't there - directory is made inside dl func
        await download_table_from_dap("users", users_folder)
    if not os.path.exists(enrollments_folder):
        await download_table_from_dap("enrollments", enrollments_folder)
    if not os.path.exists(enrollment_terms_folder):
        await download_table_from_dap("enrollment_terms", enrollment_terms_folder)
    if not os.path.exists(submissions_folder):
        await download_table_from_dap("submissions", submissions_folder)
    if not os.path.exists(courses_folder):
        await download_table_from_dap("courses", courses_folder)
    if not os.path.exists(assignments_folder):
        await download_table_from_dap("assignments", assignments_folder)
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
    if not os.path.exists(assignments_csv):
        collect_2025_records(assignments_folder, "created_at", assignments_csv)
    # TODO what can I do with incremental update from DAP? Can that update a job
    # TODO I already have?

    # either way, we need to load our csv's as dfs
    users_df = pd.read_csv(users_csv)
    enrollments_df = pd.read_csv(enrollments_csv, dtype={15: "string"}, parse_dates=[17]) # force column 14 to string and 15 to date
    submissions_df = pd.read_csv(submissions_csv, dtype={38: "string", 39: "string", 40: "string"})    # TODO <unset> row is strange, seems user id was cut off - might just be csv display weirdness
    terms_df = pd.read_csv(enrollment_terms_csv)
    courses_df = pd.read_csv(courses_csv)
    assignments_df = pd.read_csv(assignments_csv)
    
    ## grab term id & instructor id
    id_matching_name = gather_user_id_from_name(instructor_name, users_df)
    if not id_matching_name:
        print("no matching instructors found")
        sys.exit(1)
    
    id_matching_term_name = gather_term_ids_from_term_name(term_name, terms_df)
    if not id_matching_term_name:
        print("no matching instructors found")
        sys.exit(1)
    
    instructor_id = id_matching_name
    
    #print("instructor id is", instructor_id, "term id is", id_matching_term_name)
    
    ## grab a list of the courses that instructor teaches
    courses_they_teach = gather_course_ids_matching_user_id_from_enrollments(instructor_id, enrollments_df)
    if not courses_they_teach:
        print("no matching courses found")
        sys.exit(1)
    
    ## filter down that list to only courses that ran for selected term
    courses_they_teach_from_term = filter_courses_by_term(id_matching_term_name, courses_they_teach, courses_df)
    #print("they teach", courses_they_teach)
    
    ## next, get all the submissions associated with the courses in that term filtered list
    submissions_in_courses = pd.DataFrame()
    
    ## merge the dfs for the all the courses they teach in selected term together
    for i in range(len(courses_they_teach_from_term)):
        submissions_in_i = grab_submissions_for_course(courses_they_teach_from_term[i], submissions_df) # this is a df
        #print(len(submissions_in_i))
        submissions_in_courses = pd.concat([submissions_in_courses, submissions_in_i], ignore_index=True)
    
    inactive_users = get_inactive_users(courses_they_teach_from_term, enrollments_df)
    remove_inactive_users(inactive_users, submissions_in_courses)
    
    # TODO some of my ids have a .0 at the end of them in report.csv - not good - is that elsewhere too? (grader_id is what im looking at rn)
    if submissions_in_courses.empty:
        print("no matching courses found")
    else:
        # Add all name columns, week, grade delta, and compliance status to the submissions data
        submissions_enhanced = add_course_names_to_submissions(submissions_in_courses, courses_df)
        submissions_enhanced = add_instructor_names_to_submissions(submissions_enhanced, enrollments_df, users_df)
        submissions_enhanced = add_student_names_to_submissions(submissions_enhanced, users_df)
        submissions_enhanced = add_term_names_to_submissions(submissions_enhanced, courses_df, terms_df)
        submissions_enhanced = add_assignment_names_to_submissions(submissions_enhanced, assignments_df)
        submissions_enhanced = add_week_column_to_submissions(submissions_enhanced)
        submissions_enhanced = add_grade_delta_to_submissions(submissions_enhanced)
        submissions_enhanced = add_compliance_status_to_submissions(submissions_enhanced)
    
        # Reorder columns for better readability
        submissions_enhanced = reorder_columns_for_readability(submissions_enhanced)
    
        # Create dean-friendly version
        dean_report = create_dean_friendly_dataframe(submissions_enhanced)
    
        result_folder = "resulting_csv"
        os.makedirs(result_folder, exist_ok=True)
    
        # Save full detailed report
        full_result = os.path.join(result_folder, "lisa_test_report_full.csv")
        submissions_enhanced.to_csv(full_result, index=False)
    
        # Save dean-friendly report
        dean_result = os.path.join(result_folder, "lisa_test_report_dean.csv")
        dean_report.to_csv(dean_result, index=False)
    
        print("Full report saved to:", full_result)
        print("Dean-friendly report saved to:", dean_result)
        print("\nPreview of full report:")
        print(submissions_enhanced)
        # TODO make it create a raw_result.csv and a pretty_result.csv - pretty shouldn't have id's - just names and derived stats like deltas
        # TODO also go over BRO stuff and see how much of it makes sense - already seeig that its making up fields
    
        # TODO make pretty csv have choosable columns so they can configure what they want]
    
        # TODO look at example report hallie sent - want it to look something like that

        # TODO might even be able to port this by writing to sqlite db instead? How does that work with a hosted website?



if __name__ == "__main__":
    asyncio.run(main())
