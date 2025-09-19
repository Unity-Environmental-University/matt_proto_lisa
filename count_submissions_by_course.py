import json
import os
import sys

def count_submissions_for_course(course_id, submissions_folder="table_downloads/submissions"):
    """
    Count the number of submissions for a given course_id by reading through JSON files
    
    Args:
        course_id: The course ID to search for
        submissions_folder: Path to the submissions folder containing job data
    
    Returns:
        tuple: (total_submissions_count, unique_assignment_ids_set)
    """
    total_count = 0
    unique_assignment_ids = set()
    
    if not os.path.exists(submissions_folder):
        print(f"Submissions folder not found: {submissions_folder}")
        return 0, set()
    
    # Get the job folder (should be only one folder in submissions directory)
    job_folders = [f for f in os.listdir(submissions_folder) if os.path.isdir(os.path.join(submissions_folder, f))]
    
    if not job_folders:
        print(f"No job folders found in {submissions_folder}")
        return 0, set()
    
    # Use the first (and likely only) job folder
    job_folder_path = os.path.join(submissions_folder, job_folders[0])
    print(f"Searching in job folder: {job_folder_path}")
    
    # Process all JSON files in the job folder
    json_files = [f for f in os.listdir(job_folder_path) if f.endswith('.json')]
    
    if not json_files:
        print(f"No JSON files found in {job_folder_path}")
        return 0, set()
    
    print(f"Found {len(json_files)} JSON files to process")
    
    for filename in json_files:
        file_path = os.path.join(job_folder_path, filename)
        file_count = 0
        
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                for line_num, line in enumerate(f, 1):
                    if line.strip():  # Skip empty lines
                        try:
                            data = json.loads(line)
                            # Check if this submission belongs to the target course
                            if data.get("value", {}).get("course_id") == course_id:
                                file_count += 1
                                total_count += 1
                                # Track unique assignment IDs
                                assignment_id = data.get("value", {}).get("assignment_id")
                                if assignment_id is not None:
                                    unique_assignment_ids.add(assignment_id)
                        except json.JSONDecodeError as e:
                            print(f"JSON decode error in {filename} line {line_num}: {e}")
                            continue
            
            print(f"{filename}: {file_count} submissions for course {course_id}")
            
        except Exception as e:
            print(f"Error processing {filename}: {e}")
            continue
    
    return total_count, unique_assignment_ids

def main():
    # if len(sys.argv) != 2:
    #     print("Usage: python count_submissions_by_course.py <course_id>")
    #     print("Example: python count_submissions_by_course.py 4195124")
    #     sys.exit(1)
    #
    # try:
    #     course_id = int(sys.argv[1])
    # except ValueError:
    #     print("Error: course_id must be a valid integer")
    #     sys.exit(1)
    course_id = 7262009
    
    print(f"Counting submissions for course ID: {course_id}")
    
    total_submissions, unique_assignment_ids = count_submissions_for_course(course_id)
    
    print(f"\nTotal submissions found for course {course_id}: {total_submissions}")
    print(f"Unique assignment IDs in course {course_id}: {len(unique_assignment_ids)}")
    
    if unique_assignment_ids:
        print(f"Assignment IDs: {sorted(unique_assignment_ids)}")

if __name__ == "__main__":
    main()