"""
Instructor client implementation for LMS.
Provides interface for instructors to manage courses and students.
"""

import os
from typing import List, Optional
from datetime import datetime
import json
import grpc
from lms import lms_pb2, lms_pb2_grpc
#from client.base_client import BaseLMSClient
from client.enhanced_base_client import EnhancedBaseLMSClient
from common.constants import *
from common.logger import get_logger
import time


logger = get_logger(__name__)


class InstructorClient(EnhancedBaseLMSClient):
    """
    Client application for instructors.
    """
    
    def __init__(self, server_addresses: List[str]):
        """Initialize instructor client"""
        super().__init__(server_addresses, USER_TYPE_INSTRUCTOR)
    
    def show_menu(self):
        """Display instructor menu"""
        print("\n" + "="*50)
        print("INSTRUCTOR MENU")
        print("="*50)
        print("1. Create Assignment")
        print("2. View All Assignments")
        print("3. View Assignment Submissions")
        print("4. Grade Assignment")
        print("5. Upload Course Material")
        print("6. View Course Materials")
        print("7. View Student Queries")
        print("8. Answer Student Query")
        print("9. View All Students")
        print("10. View Student Progress")
        print("11. Class Statistics")
        print("0. Logout")
        print("="*50)
    
    def handle_menu_choice(self, choice: str):
        """Handle instructor menu choice"""
        if choice == "1":
            self.create_assignment()
        elif choice == "2":
            self.view_assignments()
        elif choice == "3":
            self.view_assignment_submissions()
        elif choice == "4":
            self.grade_assignment()
        elif choice == "5":
            self.upload_course_material()
        elif choice == "6":
            self.view_course_materials()
        elif choice == "7":
            self.view_student_queries()
        elif choice == "8":
            self.answer_student_query()
        elif choice == "9":
            self.view_all_students()
        elif choice == "10":
            self.view_student_progress()
        elif choice == "11":
            self.view_class_statistics()
        elif choice == "0":
            return False
        else:
            print("Invalid choice. Please try again.")
        
        return True
    
    def create_assignment(self):
        """Create a new assignment"""
        print("\n--- CREATE ASSIGNMENT ---")
        
        title = input("Assignment Title: ").strip()
        if not title:
            print("Title cannot be empty.")
            return
        
        description = input("Description: ").strip()
        if not description:
            print("Description cannot be empty.")
            return
        
        # Get due date
        print("\nDue date (format: YYYY-MM-DD HH:MM)")
        due_date = input("Due date: ").strip()
        
        # Validate date format
        try:
            datetime.strptime(due_date, "%Y-%m-%d %H:%M")
        except ValueError:
            print("Invalid date format.")
            return
        
        # Optional file attachment
        attach_file = input("\nAttach a file? (y/n): ").lower() == 'y'
        file_path = None
        
        if attach_file:
            file_path = input("File path: ").strip()
            if not os.path.exists(file_path):
                print("File not found.")
                return
        
        # Create assignment
        success, assignment_id = self.post_data(
            DATA_TYPE_ASSIGNMENT,
            {
                'title': title,
                'description': description,
                'due_date': due_date
            },
            file_path
        )
        
        if success:
            print(f"\n✅ Assignment created successfully!")
            print(f"Assignment ID: {assignment_id}")
        else:
            print("\n❌ Failed to create assignment.")
    
    def view_assignments(self):
        """View all assignments"""
        print("\n--- ALL ASSIGNMENTS ---")
        
        assignments = self.get_data(DATA_TYPE_ASSIGNMENT)
        
        if not assignments:
            print("No assignments created yet.")
            return
        
        for i, assignment in enumerate(assignments, 1):
            print(f"\n{i}. {assignment['title']}")
            print(f"   ID: {assignment['assignment_id']}")
            print(f"   Description: {assignment['description']}")
            print(f"   Due Date: {assignment['due_date']}")
            print(f"   Created: {self._format_timestamp(assignment['created_at'])}")
            if assignment.get('file_path'):
                print("   📎 Has attachment")
    
    def view_assignment_submissions(self):
        """View submissions for a specific assignment"""
        print("\n--- VIEW ASSIGNMENT SUBMISSIONS ---")
        
        # First, show assignments
        assignments = self.get_data(DATA_TYPE_ASSIGNMENT)
        
        if not assignments:
            print("No assignments available.")
            return
        
        print("\nAvailable assignments:")
        for i, assignment in enumerate(assignments, 1):
            print(f"{i}. {assignment['title']}")
        
        try:
            choice = int(input("\nSelect assignment number: ")) - 1
            if choice < 0 or choice >= len(assignments):
                print("Invalid selection.")
                return
        except ValueError:
            print("Invalid input.")
            return
        
        selected_assignment = assignments[choice]
        assignment_id = selected_assignment['assignment_id']
        
        # Get submissions for this assignment
        submissions = self.get_data(
            DATA_TYPE_SUBMISSION,
            {'assignment_id': assignment_id}
        )
        
        if not submissions:
            print(f"\nNo submissions for '{selected_assignment['title']}' yet.")
            return
        
        # Get student names
        students = self.get_data(DATA_TYPE_STUDENTS)
        student_map = {s['user_id']: s['name'] for s in students}
        
        print(f"\nSubmissions for '{selected_assignment['title']}':")
        print("-" * 50)
        
        for i, submission in enumerate(submissions, 1):
            student_name = student_map.get(submission['student_id'], 'Unknown Student')
            print(f"\n{i}. Student: {student_name}")
            print(f"   Submitted: {self._format_timestamp(submission['submitted_at'])}")
            print(f"   Content: {submission['content'][:100]}...")
            
            if submission.get('grade') is not None:
                print(f"   Grade: {submission['grade']:.1f}%")
                print(f"   Status: ✅ Graded")
            else:
                print("   Status: ⏳ Pending grading")
    
    def grade_assignment(self):
        """Grade a student's assignment submission"""
        print("\n--- GRADE ASSIGNMENT ---")
        
        # Get ungraded submissions
        all_submissions = self.get_data(DATA_TYPE_SUBMISSION)
        ungraded = [s for s in all_submissions if s.get('grade') is None]
        
        if not ungraded:
            print("No submissions pending grading.")
            return
        
        # Get assignment and student details
        assignments = self.get_data(DATA_TYPE_ASSIGNMENT)
        students = self.get_data(DATA_TYPE_STUDENTS)
        assignment_map = {a['assignment_id']: a['title'] for a in assignments}
        student_map = {s['user_id']: s['name'] for s in students}
        
        print("\nUngraded submissions:")
        for i, submission in enumerate(ungraded, 1):
            assignment_title = assignment_map.get(submission['assignment_id'], 'Unknown')
            student_name = student_map.get(submission['student_id'], 'Unknown')
            print(f"{i}. {student_name} - {assignment_title}")
            print(f"   Submitted: {self._format_timestamp(submission['submitted_at'])}")
        
        try:
            choice = int(input("\nSelect submission to grade: ")) - 1
            if choice < 0 or choice >= len(ungraded):
                print("Invalid selection.")
                return
        except ValueError:
            print("Invalid input.")
            return
        
        selected_submission = ungraded[choice]
        
        # Show submission details
        print(f"\n--- SUBMISSION DETAILS ---")
        print(f"Student: {student_map.get(selected_submission['student_id'], 'Unknown')}")
        print(f"Assignment: {assignment_map.get(selected_submission['assignment_id'], 'Unknown')}")
        print(f"\nSubmission Content:")
        print("-" * 50)
        print(selected_submission['content'])
        print("-" * 50)
        
        # Get grade
        try:
            grade = float(input("\nGrade (0-100): "))
            if grade < 0 or grade > 100:
                print("Grade must be between 0 and 100.")
                return
        except ValueError:
            print("Invalid grade.")
            return
        
        # Get feedback
        feedback = input("Feedback (optional): ").strip()
        
        # Submit grade using the correct RPC
        try:
            # import lms_pb2
            from lms import lms_pb2

            request = lms_pb2.GradeRequest(
                token=self.token,
                assignment_id=selected_submission['assignment_id'],
                student_id=selected_submission['student_id'],
                grade=grade,
                feedback=feedback
            )
            
            response = self.stub.SubmitGrade(request)
            
            if response.success:
                print(f"\n✅ Grade submitted successfully!")
                print(f"Grade: {grade:.1f}%")
            else:
                print(f"\n❌ Failed to submit grade: {response.error}")
                
        except Exception as e:
            logger.error(f"Grade submission error: {e}")
            print("\n❌ Failed to submit grade.")
    
    def upload_course_material(self):
        """Upload course material"""
        print("\n--- UPLOAD COURSE MATERIAL ---")
        
        file_path = input("File path (PDF or TXT): ").strip()
        
        if not os.path.exists(file_path):
            print("File not found.")
            return
        
        # Check file type
        if not (file_path.lower().endswith('.pdf') or file_path.lower().endswith('.txt')):
            print("Only PDF and TXT files are allowed.")
            return
        
        # Get topic
        topic = input("Topic/Category: ").strip()
        if not topic:
            topic = "General"
        
        # Upload
        success, material_id = self.post_data(
            DATA_TYPE_COURSE_MATERIAL,
            {'topic': topic},
            file_path
        )
        
        if success:
            print(f"\n✅ Course material uploaded successfully!")
            print(f"Material ID: {material_id}")
        else:
            print("\n❌ Failed to upload course material.")
    
    def view_course_materials(self):
        """View all course materials"""
        print("\n--- COURSE MATERIALS ---")
        
        materials = self.get_data(DATA_TYPE_COURSE_MATERIAL)
        
        if not materials:
            print("No course materials uploaded yet.")
            return
        
        for i, material in enumerate(materials, 1):
            print(f"\n{i}. {material['filename']}")
            print(f"   ID: {material['material_id']}")
            print(f"   Topic: {material['topic']}")
            print(f"   Type: {material['file_type']}")
            print(f"   Uploaded: {self._format_timestamp(material['uploaded_at'])}")
    
    def view_student_queries(self):
        """View all student queries"""
        print("\n--- STUDENT QUERIES ---")
        
        queries = self.get_data(DATA_TYPE_QUERY)
        
        if not queries:
            print("No student queries yet.")
            return
        
        # Get student names
        students = self.get_data(DATA_TYPE_STUDENTS)
        student_map = {s['user_id']: s['name'] for s in students}
        
        # Separate by status
        unanswered = [q for q in queries if not q.get('answer')]
        answered = [q for q in queries if q.get('answer')]
        
        if unanswered:
            print("\n📌 UNANSWERED QUERIES:")
            print("-" * 50)
            for i, query in enumerate(unanswered, 1):
                student_name = student_map.get(query['student_id'], 'Unknown')
                print(f"\n{i}. From: {student_name}")
                print(f"   Query: {query['query']}")
                print(f"   Posted: {self._format_timestamp(query['posted_at'])}")
                print(f"   Type: {'AI Tutor Request' if query['use_llm'] else 'Instructor Request'}")
        
        if answered:
            print("\n✅ ANSWERED QUERIES:")
            print("-" * 50)
            for i, query in enumerate(answered, 1):
                student_name = student_map.get(query['student_id'], 'Unknown')
                print(f"\n{i}. From: {student_name}")
                print(f"   Query: {query['query']}")
                print(f"   Answer: {query['answer'][:100]}...")
                print(f"   Answered by: {query.get('answered_by', 'Unknown')}")
    
    def answer_student_query(self):
        """Answer a student query"""
        print("\n--- ANSWER STUDENT QUERY ---")
        
        # Get unanswered queries that need instructor response
        all_queries = self.get_data(DATA_TYPE_QUERY)
        unanswered = [q for q in all_queries 
                     if not q.get('answer') and not q.get('use_llm')]
        
        if not unanswered:
            print("No queries pending instructor response.")
            return
        
        # Get student names
        students = self.get_data(DATA_TYPE_STUDENTS)
        student_map = {s['user_id']: s['name'] for s in students}
        
        print("\nQueries requiring instructor response:")
        for i, query in enumerate(unanswered, 1):
            student_name = student_map.get(query['student_id'], 'Unknown')
            print(f"{i}. From: {student_name}")
            print(f"   Query: {query['query']}")
            print(f"   Posted: {self._format_timestamp(query['posted_at'])}")
        
        try:
            choice = int(input("\nSelect query to answer: ")) - 1
            if choice < 0 or choice >= len(unanswered):
                print("Invalid selection.")
                return
        except ValueError:
            print("Invalid input.")
            return
        
        selected_query = unanswered[choice]
        
        # Get answer
        print(f"\nQuery: {selected_query['query']}")
        print("\nYour answer:")
        answer = input("> ").strip()
        
        if not answer:
            print("Answer cannot be empty.")
            return
        
        # Submit answer
        try:
            # Use the database method to answer query
            # We need to create a command for this
            success = self.stub is not None  # Check connection
            
            if success:
                # Since we don't have a direct RPC for answering queries,
                # we'll use the LLMQueryRequest (even though it's from instructor)
                # import lms_pb2
                from lms import lms_pb2

                request = lms_pb2.LLMQueryRequest(
                    query_id=selected_query['query_id'],
                    query=selected_query['query'],
                    context=json.dumps({
                        'answer': answer,
                        'answered_by': USER_TYPE_INSTRUCTOR
                    })
                )
                
                response = self.stub.GetLLMAnswer(request)
                
                if response.success:
                    print("\n✅ Answer submitted successfully!")
                else:
                    print("\n❌ Failed to submit answer.")
            else:
                print("\n❌ Not connected to server.")
                
        except Exception as e:
            logger.error(f"Answer submission error: {e}")
            print("\n❌ Failed to submit answer.")
    
    def view_all_students(self):
        """View all students in the class"""
        print("\n--- ALL STUDENTS ---")
        
        students = self.get_data(DATA_TYPE_STUDENTS)
        
        if not students:
            print("No students enrolled yet.")
            return
        
        print(f"\nTotal Students: {len(students)}")
        print("-" * 50)
        
        for i, student in enumerate(students, 1):
            print(f"\n{i}. {student['name']}")
            print(f"   Username: {student['username']}")
            print(f"   User ID: {student['user_id']}")
            
            # Show progress if available
            if 'progress' in student:
                progress = student['progress']
                print(f"   Level: {progress.get('level', 'BEGINNER')}")
                print(f"   Average Grade: {progress.get('average_grade', 0):.1f}%")
    
    def view_student_progress(self):
        """View detailed progress for a specific student"""
        print("\n--- VIEW STUDENT PROGRESS ---")
        
        students = self.get_data(DATA_TYPE_STUDENTS)
        
        if not students:
            print("No students enrolled yet.")
            return
        
        # Show student list
        print("\nSelect a student:")
        for i, student in enumerate(students, 1):
            print(f"{i}. {student['name']} ({student['username']})")
        
        try:
            choice = int(input("\nSelect student number: ")) - 1
            if choice < 0 or choice >= len(students):
                print("Invalid selection.")
                return
        except ValueError:
            print("Invalid input.")
            return
        
        selected_student = students[choice]
        student_id = selected_student['user_id']
        
        # Get student's submissions
        all_submissions = self.get_data(DATA_TYPE_SUBMISSION)
        student_submissions = [s for s in all_submissions 
                             if s['student_id'] == student_id]
        
        # Get assignments
        assignments = self.get_data(DATA_TYPE_ASSIGNMENT)
        assignment_map = {a['assignment_id']: a['title'] for a in assignments}
        
        print(f"\n--- Progress for {selected_student['name']} ---")
        print(f"Username: {selected_student['username']}")
        
        # Overall statistics
        total_assignments = len(assignments)
        submitted_assignments = len(set(s['assignment_id'] for s in student_submissions))
        graded_submissions = [s for s in student_submissions if s.get('grade') is not None]
        
        print(f"\nAssignment Progress:")
        print(f"  Total Assignments: {total_assignments}")
        print(f"  Submitted: {submitted_assignments}")
        print(f"  Completion Rate: {(submitted_assignments/total_assignments*100) if total_assignments > 0 else 0:.1f}%")
        
        if graded_submissions:
            total_grade = sum(s['grade'] for s in graded_submissions)
            average_grade = total_grade / len(graded_submissions)
            print(f"\nGrade Information:")
            print(f"  Graded Assignments: {len(graded_submissions)}")
            print(f"  Average Grade: {average_grade:.1f}%")
            
            # Show individual grades
            print("\nDetailed Grades:")
            for submission in graded_submissions:
                assignment_title = assignment_map.get(submission['assignment_id'], 'Unknown')
                print(f"  - {assignment_title}: {submission['grade']:.1f}%")
        
        # Show queries
        all_queries = self.get_data(DATA_TYPE_QUERY)
        student_queries = [q for q in all_queries if q['student_id'] == student_id]
        
        if student_queries:
            print(f"\nQueries Posted: {len(student_queries)}")
            answered = len([q for q in student_queries if q.get('answer')])
            print(f"  Answered: {answered}")
    
    def view_class_statistics(self):
        """View overall class statistics"""
        print("\n--- CLASS STATISTICS ---")
        
        # Get all data
        students = self.get_data(DATA_TYPE_STUDENTS)
        assignments = self.get_data(DATA_TYPE_ASSIGNMENT)
        submissions = self.get_data(DATA_TYPE_SUBMISSION)
        queries = self.get_data(DATA_TYPE_QUERY)
        materials = self.get_data(DATA_TYPE_COURSE_MATERIAL)
        
        print(f"\nClass Overview:")
        print(f"  Total Students: {len(students)}")
        print(f"  Total Assignments: {len(assignments)}")
        print(f"  Total Submissions: {len(submissions)}")
        print(f"  Course Materials: {len(materials)}")
        print(f"  Student Queries: {len(queries)}")
        
        # Grade statistics
        graded_submissions = [s for s in submissions if s.get('grade') is not None]
        if graded_submissions:
            grades = [s['grade'] for s in graded_submissions]
            average_grade = sum(grades) / len(grades)
            
            print(f"\nGrade Statistics:")
            print(f"  Graded Submissions: {len(graded_submissions)}")
            print(f"  Class Average: {average_grade:.1f}%")
            print(f"  Highest Grade: {max(grades):.1f}%")
            print(f"  Lowest Grade: {min(grades):.1f}%")
            
            # Grade distribution
            print("\nGrade Distribution:")
            a_grades = len([g for g in grades if g >= 90])
            b_grades = len([g for g in grades if 80 <= g < 90])
            c_grades = len([g for g in grades if 70 <= g < 80])
            d_grades = len([g for g in grades if 60 <= g < 70])
            f_grades = len([g for g in grades if g < 60])
            
            print(f"  A (90-100): {a_grades} students")
            print(f"  B (80-89): {b_grades} students")
            print(f"  C (70-79): {c_grades} students")
            print(f"  D (60-69): {d_grades} students")
            print(f"  F (0-59): {f_grades} students")
        
        # Submission statistics
        if assignments and students:
            total_possible = len(assignments) * len(students)
            submission_rate = (len(submissions) / total_possible * 100) if total_possible > 0 else 0
            print(f"\nSubmission Rate: {submission_rate:.1f}%")
        
        # Query statistics
        if queries:
            answered_queries = len([q for q in queries if q.get('answer')])
            llm_queries = len([q for q in queries if q.get('use_llm')])
            
            print(f"\nQuery Statistics:")
            print(f"  Total Queries: {len(queries)}")
            print(f"  Answered: {answered_queries}")
            print(f"  AI Tutor Queries: {llm_queries}")
            print(f"  Instructor Queries: {len(queries) - llm_queries}")
    
    def _format_timestamp(self, timestamp: float) -> str:
        """Format timestamp for display"""
        if timestamp:
            return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")
        return "Unknown"


def main():
    """Main function for instructor client"""
    print("\n" + "="*50)
    print("DISTRIBUTED LMS - INSTRUCTOR CLIENT")
    print("="*50)
    
    # Load configuration to get proper server addresses
    try:
        import json
        import os
        
        # Get config path relative to project root
        config_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 
            'config.json'
        )
        
        with open(config_path, 'r') as f:
            config = json.load(f)
        
        # Extract server addresses from config
        server_addresses = []
        nodes = config['cluster']['nodes']
        for node_id, node_config in nodes.items():
            address = f"{node_config['host']}:{node_config['lms_port']}"
            server_addresses.append(address)
            
        logger.info(f"Loaded server addresses from config: {server_addresses}")
        
    except Exception as e:
        logger.warning(f"Could not load config, using defaults: {e}")
        # Fallback to default addresses
        server_addresses = [
            "localhost:6001",
            "localhost:6002", 
            "localhost:6003",
            "localhost:6004"
        ]
    
    # Create client
    client = InstructorClient(server_addresses)
    
    # Pass config to client for leader discovery
    if 'config' in locals():
        client.set_config(config)

    if not client.is_connected():
        print("Failed to connect to any LMS server.")
        return
    
    # Login loop
    while True:
        print("\nPlease login to continue")
        username = input("Username: ")
        password = input("Password: ")
        
        if client.login(username, password):
            print(f"\nWelcome, {username}!")
            break
        else:
            print("Login failed. Please try again.")
            retry = input("Try again? (y/n): ")
            if retry.lower() != 'y':
                return
    
    # Main menu loop
    while True:
        client.show_menu()
        choice = input("\nEnter your choice: ")
        
        if not client.handle_menu_choice(choice):
            break
    
    # Logout
    client.logout()
    print("\nThank you for using the LMS. Goodbye!")


if __name__ == "__main__":
    main()