"""
Student client implementation for LMS.
Provides interface for students to interact with the system.
"""

import os
from typing import List, Optional
from datetime import datetime

from client.base_client import BaseLMSClient
from common.constants import *
from common.logger import get_logger

logger = get_logger(__name__)


class StudentClient(BaseLMSClient):
    """
    Client application for students.
    """
    
    def __init__(self, server_addresses: List[str]):
        """Initialize student client"""
        super().__init__(server_addresses, USER_TYPE_STUDENT)
    
    def show_menu(self):
        """Display student menu"""
        print("\n" + "="*50)
        print("STUDENT MENU")
        print("="*50)
        print("1. View Assignments")
        print("2. Submit Assignment")
        print("3. View My Submissions")
        print("4. View My Grades")
        print("5. View Course Materials")
        print("6. Download Course Material")
        print("7. Post Query")
        print("8. View My Queries")
        print("9. View My Progress")
        print("0. Logout")
        print("="*50)
    
    def handle_menu_choice(self, choice: str):
        """Handle student menu choice"""
        if choice == "1":
            self.view_assignments()
        elif choice == "2":
            self.submit_assignment()
        elif choice == "3":
            self.view_submissions()
        elif choice == "4":
            self.view_grades()
        elif choice == "5":
            self.view_course_materials()
        elif choice == "6":
            self.download_course_material()
        elif choice == "7":
            self.post_query()
        elif choice == "8":
            self.view_queries()
        elif choice == "9":
            self.view_progress()
        elif choice == "0":
            return False
        else:
            print("Invalid choice. Please try again.")
        
        return True
    
    def view_assignments(self):
        """View all assignments"""
        print("\n--- ASSIGNMENTS ---")
        
        assignments = self.get_data(DATA_TYPE_ASSIGNMENT)
        
        if not assignments:
            print("No assignments available.")
            return
        
        for i, assignment in enumerate(assignments, 1):
            print(f"\n{i}. {assignment['title']}")
            print(f"   Description: {assignment['description']}")
            print(f"   Due Date: {assignment['due_date']}")
            if assignment.get('file_path'):
                print("   📎 Has attachment")
    
    def submit_assignment(self):
        """Submit an assignment"""
        print("\n--- SUBMIT ASSIGNMENT ---")
        
        # First, show available assignments
        assignments = self.get_data(DATA_TYPE_ASSIGNMENT)
        
        if not assignments:
            print("No assignments available.")
            return
        
        print("\nAvailable assignments:")
        for i, assignment in enumerate(assignments, 1):
            print(f"{i}. {assignment['title']} (Due: {assignment['due_date']})")
        
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
        
        # Check if already submitted
        my_submissions = self.get_data(DATA_TYPE_SUBMISSION)
        already_submitted = any(
            sub['assignment_id'] == assignment_id 
            for sub in my_submissions
        )
        
        if already_submitted:
            print("\n⚠️  You have already submitted this assignment.")
            confirm = input("Do you want to submit again? (y/n): ")
            if confirm.lower() != 'y':
                return
        
        # Get submission content
        print("\nEnter your submission:")
        print("(Type your answer, or enter 'file' to upload a file)")
        
        content = input("> ")
        file_path = None
        
        if content.lower() == 'file':
            file_path = input("Enter file path: ").strip()
            if not os.path.exists(file_path):
                print("File not found.")
                return
            content = f"File submission: {os.path.basename(file_path)}"
        
        # Submit
        success, submission_id = self.post_data(
            DATA_TYPE_SUBMISSION,
            {
                'assignment_id': assignment_id,
                'content': content
            },
            file_path
        )
        
        if success:
            print(f"\n✅ Assignment submitted successfully!")
            print(f"Submission ID: {submission_id}")
        else:
            print("\n❌ Failed to submit assignment.")
    
    def view_submissions(self):
        """View student's submissions"""
        print("\n--- MY SUBMISSIONS ---")
        
        submissions = self.get_data(DATA_TYPE_SUBMISSION)
        
        if not submissions:
            print("You haven't submitted any assignments yet.")
            return
        
        # Get assignment details for titles
        assignments = self.get_data(DATA_TYPE_ASSIGNMENT)
        assignment_map = {a['assignment_id']: a['title'] for a in assignments}
        
        for i, submission in enumerate(submissions, 1):
            assignment_title = assignment_map.get(submission['assignment_id'], 'Unknown')
            print(f"\n{i}. Assignment: {assignment_title}")
            print(f"   Submitted: {self._format_timestamp(submission['submitted_at'])}")
            print(f"   Content: {submission['content'][:100]}...")
            
            if submission.get('grade') is not None:
                print(f"   Grade: {submission['grade']:.1f}%")
                if submission.get('feedback'):
                    print(f"   Feedback: {submission['feedback']}")
            else:
                print("   Status: Pending grading")
    
    def view_grades(self):
        """View student's grades"""
        print("\n--- MY GRADES ---")
        
        submissions = self.get_data(DATA_TYPE_SUBMISSION)
        graded_submissions = [s for s in submissions if s.get('grade') is not None]
        
        if not graded_submissions:
            print("You don't have any graded assignments yet.")
            return
        
        # Get assignment details
        assignments = self.get_data(DATA_TYPE_ASSIGNMENT)
        assignment_map = {a['assignment_id']: a['title'] for a in assignments}
        
        total_grade = 0
        print("\nGraded Assignments:")
        print("-" * 50)
        
        for submission in graded_submissions:
            assignment_title = assignment_map.get(submission['assignment_id'], 'Unknown')
            print(f"\n{assignment_title}")
            print(f"Grade: {submission['grade']:.1f}%")
            if submission.get('feedback'):
                print(f"Feedback: {submission['feedback']}")
            print(f"Graded on: {self._format_timestamp(submission.get('graded_at', 0))}")
            total_grade += submission['grade']
        
        if graded_submissions:
            average_grade = total_grade / len(graded_submissions)
            print("\n" + "-" * 50)
            print(f"Average Grade: {average_grade:.1f}%")
            
            # Determine level
            if average_grade >= GRADE_THRESHOLD_ADVANCED:
                level = "ADVANCED"
            elif average_grade >= GRADE_THRESHOLD_INTERMEDIATE:
                level = "INTERMEDIATE"
            else:
                level = "BEGINNER"
            
            print(f"Current Level: {level}")
    
    def view_course_materials(self):
        """View available course materials"""
        print("\n--- COURSE MATERIALS ---")
        
        materials = self.get_data(DATA_TYPE_COURSE_MATERIAL)
        
        if not materials:
            print("No course materials available.")
            return
        
        for i, material in enumerate(materials, 1):
            print(f"\n{i}. {material['filename']}")
            print(f"   Topic: {material['topic']}")
            print(f"   Type: {material['file_type']}")
            print(f"   Uploaded: {self._format_timestamp(material['uploaded_at'])}")
    
    def download_course_material(self):
        """Download a course material"""
        print("\n--- DOWNLOAD COURSE MATERIAL ---")
        
        materials = self.get_data(DATA_TYPE_COURSE_MATERIAL)
        
        if not materials:
            print("No course materials available.")
            return
        
        # Show materials
        for i, material in enumerate(materials, 1):
            print(f"{i}. {material['filename']} ({material['topic']})")
        
        try:
            choice = int(input("\nSelect material number: ")) - 1
            if choice < 0 or choice >= len(materials):
                print("Invalid selection.")
                return
        except ValueError:
            print("Invalid input.")
            return
        
        selected_material = materials[choice]
        
        # Download
        save_dir = "downloads"
        os.makedirs(save_dir, exist_ok=True)
        save_path = os.path.join(save_dir, selected_material['filename'])
        
        print(f"\nDownloading {selected_material['filename']}...")
        
        success = self.download_file(
            selected_material['material_id'],
            DATA_TYPE_COURSE_MATERIAL,
            save_path
        )
        
        if success:
            print(f"✅ Downloaded to: {save_path}")
        else:
            print("❌ Download failed.")
    
    def post_query(self):
        """Post a query"""
        print("\n--- POST QUERY ---")
        
        query = input("\nEnter your question: ").strip()
        if not query:
            print("Query cannot be empty.")
            return
        
        # Ask if they want LLM or instructor response
        print("\nWho should answer your query?")
        print("1. AI Tutor (instant response)")
        print("2. Instructor (may take time)")
        
        choice = input("Select (1 or 2): ").strip()
        use_llm = (choice == "1")
        
        # Post query
        success, query_id = self.post_data(
            DATA_TYPE_QUERY,
            {
                'query': query,
                'use_llm': use_llm
            }
        )
        
        if success:
            print(f"\n✅ Query posted successfully!")
            print(f"Query ID: {query_id}")
            if use_llm:
                print("The AI tutor is preparing your answer...")
            else:
                print("Your instructor will answer soon.")
        else:
            print("\n❌ Failed to post query.")
    
    def view_queries(self):
        """View student's queries"""
        print("\n--- MY QUERIES ---")
        
        queries = self.get_data(DATA_TYPE_QUERY)
        
        if not queries:
            print("You haven't posted any queries yet.")
            return
        
        for i, query in enumerate(queries, 1):
            print(f"\n{i}. Query: {query['query']}")
            print(f"   Posted: {self._format_timestamp(query['posted_at'])}")
            print(f"   Type: {'AI Tutor' if query['use_llm'] else 'Instructor'}")
            
            if query.get('answer'):
                print(f"   Status: Answered by {query.get('answered_by', 'Unknown')}")
                print(f"   Answer: {query['answer'][:200]}...")
                if len(query['answer']) > 200:
                    show_full = input("   Show full answer? (y/n): ")
                    if show_full.lower() == 'y':
                        print(f"\n   Full Answer:\n   {query['answer']}")
            else:
                print("   Status: Waiting for answer")
    
    def view_progress(self):
        """View student's progress"""
        print("\n--- MY PROGRESS ---")
        
        # Get all submissions
        submissions = self.get_data(DATA_TYPE_SUBMISSION)
        assignments = self.get_data(DATA_TYPE_ASSIGNMENT)
        queries = self.get_data(DATA_TYPE_QUERY)
        
        # Calculate statistics
        total_assignments = len(assignments)
        submitted_assignments = len(set(s['assignment_id'] for s in submissions))
        graded_submissions = [s for s in submissions if s.get('grade') is not None]
        
        print(f"\nAssignments:")
        print(f"  Total: {total_assignments}")
        print(f"  Submitted: {submitted_assignments}")
        print(f"  Completion Rate: {(submitted_assignments/total_assignments*100) if total_assignments > 0 else 0:.1f}%")
        
        if graded_submissions:
            total_grade = sum(s['grade'] for s in graded_submissions)
            average_grade = total_grade / len(graded_submissions)
            
            print(f"\nGrades:")
            print(f"  Graded Assignments: {len(graded_submissions)}")
            print(f"  Average Grade: {average_grade:.1f}%")
            
            # Determine level
            if average_grade >= GRADE_THRESHOLD_ADVANCED:
                level = "ADVANCED"
                message = "Excellent work! You're mastering the material."
            elif average_grade >= GRADE_THRESHOLD_INTERMEDIATE:
                level = "INTERMEDIATE"
                message = "Good progress! Keep up the good work."
            else:
                level = "BEGINNER"
                message = "You're learning! Don't hesitate to ask questions."
            
            print(f"  Current Level: {level}")
            print(f"  {message}")
        
        print(f"\nQueries:")
        print(f"  Total Posted: {len(queries)}")
        answered_queries = len([q for q in queries if q.get('answer')])
        print(f"  Answered: {answered_queries}")
        
        print("\n" + "-" * 50)
        print("Keep learning and improving! 📚")
    
    def _format_timestamp(self, timestamp: float) -> str:
        """Format timestamp for display"""
        if timestamp:
            return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")
        return "Unknown"


def main():
    """Main function for student client"""
    print("\n" + "="*50)
    print("DISTRIBUTED LMS - STUDENT CLIENT")
    print("="*50)
    
    # Get server addresses from config or use defaults
    server_addresses = [
        "localhost:6001",
        "localhost:6002",
        "localhost:6003",
        "localhost:6004",
        "localhost:6005"
    ]
    
    # Create client
    client = StudentClient(server_addresses)
    
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