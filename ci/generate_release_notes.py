#!/usr/bin/env python3
"""
Script to generate release notes from git commits and pull requests
"""

import argparse
import subprocess
import re
import os
import sys
from datetime import datetime
import json
import urllib.request
import urllib.error

def get_github_api_data(url, token):
    """Fetch data from GitHub API"""
    headers = {
        'Authorization': f'token {token}',
        'Accept': 'application/vnd.github.v3+json'
    }
    
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req) as response:
            return json.loads(response.read().decode())
    except urllib.error.HTTPError as e:
        print(f"Error accessing GitHub API: {e}", file=sys.stderr)
        return None

def get_commits_since_last_tag(tag):
    """Get all commits since the last tag"""
    try:
        # Get the previous tag
        tags = subprocess.check_output(['git', 'tag', '--sort=-version:refname']).decode().strip().split('\n')
        
        current_tag_index = tags.index(tag) if tag in tags else 0
        previous_tag = tags[current_tag_index + 1] if current_tag_index + 1 < len(tags) else None
        
        if previous_tag:
            # Get commits between tags
            commit_range = f"{previous_tag}..{tag}"
        else:
            # Get all commits up to this tag
            commit_range = tag
        
        # Get commit messages
        commits = subprocess.check_output([
            'git', 'log', commit_range, 
            '--pretty=format:%H|%s|%an|%ae',
            '--no-merges'
        ]).decode().strip()
        
        if not commits:
            return [], previous_tag
        
        commit_list = []
        for line in commits.split('\n'):
            if line:
                parts = line.split('|')
                if len(parts) >= 4:
                    commit_list.append({
                        'sha': parts[0],
                        'message': parts[1],
                        'author': parts[2],
                        'email': parts[3]
                    })
        
        return commit_list, previous_tag
        
    except subprocess.CalledProcessError as e:
        print(f"Error getting commits: {e}", file=sys.stderr)
        return [], None

def categorize_commits(commits):
    """Categorize commits based on conventional commit format"""
    categories = {
        'Features': [],
        'Bug Fixes': [],
        'Performance': [],
        'Documentation': [],
        'Refactoring': [],
        'Tests': [],
        'Chores': [],
        'Other': []
    }
    
    for commit in commits:
        message = commit['message']
        
        # Parse conventional commit format
        if message.startswith('feat:') or message.startswith('feature:'):
            categories['Features'].append(commit)
        elif message.startswith('fix:') or message.startswith('bugfix:'):
            categories['Bug Fixes'].append(commit)
        elif message.startswith('perf:') or message.startswith('performance:'):
            categories['Performance'].append(commit)
        elif message.startswith('docs:') or message.startswith('doc:'):
            categories['Documentation'].append(commit)
        elif message.startswith('refactor:'):
            categories['Refactoring'].append(commit)
        elif message.startswith('test:') or message.startswith('tests:'):
            categories['Tests'].append(commit)
        elif message.startswith('chore:') or message.startswith('ci:') or message.startswith('build:'):
            categories['Chores'].append(commit)
        else:
            categories['Other'].append(commit)
    
    return categories

def extract_pr_number(message):
    """Extract PR number from commit message"""
    # Look for patterns like (#123) or #123
    match = re.search(r'#(\d+)', message)
    if match:
        return match.group(1)
    return None

def generate_release_notes(tag, repo, token, commits, previous_tag):
    """Generate release notes in Markdown format"""
    
    notes = []
    notes.append(f"# Release {tag}")
    notes.append("")
    notes.append(f"Released on {datetime.now().strftime('%Y-%m-%d')}")
    notes.append("")
    
    if previous_tag:
        notes.append(f"## Changes since {previous_tag}")
    else:
        notes.append("## Changes")
    notes.append("")
    
    # Categorize commits
    categories = categorize_commits(commits)
    
    # Add categorized commits to release notes
    for category, category_commits in categories.items():
        if category_commits:
            notes.append(f"### {category}")
            notes.append("")
            
            for commit in category_commits:
                message = commit['message']
                sha = commit['sha'][:7]
                
                # Clean up the message (remove prefix)
                cleaned_message = re.sub(r'^(feat|fix|docs|style|refactor|perf|test|chore|ci|build):\s*', '', message, flags=re.IGNORECASE)
                
                # Extract PR number if present
                pr_number = extract_pr_number(message)
                
                if pr_number and token:
                    # Try to get PR information from GitHub API
                    pr_url = f"https://api.github.com/repos/{repo}/pulls/{pr_number}"
                    pr_data = get_github_api_data(pr_url, token)
                    
                    if pr_data:
                        pr_author = pr_data.get('user', {}).get('login', commit['author'])
                        pr_title = pr_data.get('title', cleaned_message)
                        notes.append(f"- {pr_title} (#{pr_number}) by @{pr_author}")
                    else:
                        notes.append(f"- {cleaned_message} ({sha}) by {commit['author']}")
                else:
                    notes.append(f"- {cleaned_message} ({sha}) by {commit['author']}")
            
            notes.append("")
    
    # Add contributors section
    contributors = set()
    for commit in commits:
        contributors.add(commit['author'])
    
    if contributors:
        notes.append("## Contributors")
        notes.append("")
        notes.append(f"Thanks to the following contributors for this release:")
        notes.append("")
        for contributor in sorted(contributors):
            notes.append(f"- {contributor}")
        notes.append("")
    
    # Add footer
    notes.append("---")
    notes.append("")
    notes.append("For more details, see the [full changelog](https://github.com/{}/compare/{}...{})".format(
        repo,
        previous_tag if previous_tag else 'main',
        tag
    ))
    
    return '\n'.join(notes)

def main():
    parser = argparse.ArgumentParser(description='Generate release notes')
    parser.add_argument('--tag', required=True, help='Release tag')
    parser.add_argument('--repo', required=True, help='GitHub repository (owner/repo)')
    parser.add_argument('--token', help='GitHub token for API access')
    parser.add_argument('--output', default='release_notes.md', help='Output file (default: release_notes.md)')
    
    args = parser.parse_args()
    
    # Get commits since last tag
    commits, previous_tag = get_commits_since_last_tag(args.tag)
    
    if not commits:
        print("Warning: No commits found for this release", file=sys.stderr)
        commits = []
    
    # Generate release notes
    release_notes = generate_release_notes(
        args.tag,
        args.repo,
        args.token,
        commits,
        previous_tag
    )
    
    # Write to file
    with open(args.output, 'w') as f:
        f.write(release_notes)
    
    print(f"Release notes written to {args.output}")
    
    # Also output to stdout for debugging
    if os.environ.get('GITHUB_ACTIONS'):
        print("\n--- Release Notes ---")
        print(release_notes)

if __name__ == '__main__':
    main()