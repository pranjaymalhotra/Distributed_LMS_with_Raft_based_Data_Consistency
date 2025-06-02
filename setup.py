"""
Setup script for the Distributed LMS with Raft Consensus
"""

from setuptools import setup, find_packages
import os

# Read README for long description
def read_readme():
    with open('README.md', 'r', encoding='utf-8') as f:
        return f.read()

# Read requirements
def read_requirements():
    with open('requirements.txt', 'r') as f:
        return [line.strip() for line in f 
                if line.strip() and not line.startswith('#')]

setup(
    name='distributed-lms',
    version='1.0.0',
    author='Your Name',
    author_email='your.email@example.com',
    description='A distributed Learning Management System with Raft consensus and LLM-based tutoring',
    long_description=read_readme(),
    long_description_content_type='text/markdown',
    url='https://github.com/yourusername/distributed-lms',
    packages=find_packages(),
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Education',
        'Topic :: Education',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
    ],
    python_requires='>=3.8',
    install_requires=read_requirements(),
    entry_points={
        'console_scripts': [
            'lms-student=client.student_client:main',
            'lms-instructor=client.instructor_client:main',
            'lms-cluster-start=scripts.start_cluster:main',
            'lms-cluster-stop=scripts.stop_cluster:main',
        ],
    },
    include_package_data=True,
    package_data={
        'protos': ['*.proto'],
    },
    project_urls={
        'Bug Reports': 'https://github.com/yourusername/distributed-lms/issues',
        'Source': 'https://github.com/yourusername/distributed-lms',
    },
)