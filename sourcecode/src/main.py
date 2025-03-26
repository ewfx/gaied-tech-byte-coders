import os
import json
import time
import hashlib
from fastapi import FastAPI
from .filereader import read_all_emails_in_folder
from .llm import EmailClassifier
from multiprocessing import Pool
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

app = FastAPI()
classifier = EmailClassifier()  # Load LLM once

# Store seen email hashes to detect duplicates
seen_emails = set()

def generate_email_hash(email_content):
    """Generate a unique hash for each email to detect duplicates."""
    return hashlib.sha256(email_content[:500].encode()).hexdigest()

@app.get("/classify-emails/")
def classify_emails():
    """Reads emails, classifies them using LLM, checks duplicates, and returns results."""
    # Get absolute path
    folder_path = os.path.abspath("sourcecode/src/resources/emails")
    if not os.path.exists(folder_path):
        return {"error": f"Folder not found: {folder_path}"}

    start_time = time.time()

    emails = read_all_emails_in_folder(folder_path)  
    
    # Generate hashes for duplicate detection
    with ThreadPoolExecutor(max_workers=4) as executor:
        email_hashes = dict(zip(
            [email["file_name"] for email in emails],
            executor.map(generate_email_hash, [email["content"] for email in emails])
        ))

   
    # Run LLM classification in parallel
    with ThreadPoolExecutor(max_workers=4) as executor:
        results = list(executor.map(
            lambda email: classifier.classify_email(email["content"], email["file_name"]),
            emails
        ))

    # Verify LLM results with rule-based classification
    with ThreadPoolExecutor(max_workers=4) as executor:
        rule_results = list(executor.map(
            lambda email: classifier.rule_based_classification(email["content"]),
            emails
        ))

    # # Use multi-threading to speed up LLM classification
    # with ThreadPoolExecutor(max_workers=4) as executor:
    #     results = list(executor.map(lambda email: classifier.classify_email(email["content"], email["file_name"]), emails))

    # Store classification results
    classified_emails = []
    for email, llm_result, rule_result in zip(emails, results, rule_results):
        email_hash = email_hashes[email["file_name"]]
        is_duplicate = email_hash in seen_emails  
        if not is_duplicate:  
            seen_emails.add(email_hash)

        # Verify LLM output with rule-based classification
        classification = classifier.verify_llm_with_rules(llm_result, rule_result)

        print(classification)
        classified_emails.append({
            "file_name": email["file_name"],
            "classification" : classification,
            "duplicate" : is_duplicate
        })

    end_time = time.time()
    print(f"Classification completed in {end_time - start_time:.2f} seconds")

    return {"emails": classified_emails, "processing_time": f"{end_time - start_time:.2f} seconds"}
