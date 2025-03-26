import json
import re
import ollama
import os

class EmailClassifier:

    def __init__(self, model="gemma:2b", examples_file="./resources/request_types.json"):
        """Initialize classifier with LLM model and request type rules."""
        self.model = model

        # Get absolute path
        base_dir = os.path.dirname(os.path.abspath(__file__))  # Directory of this script
        file_path = os.path.join(base_dir, examples_file)

        # Load examples
        self.examples = self.load_examples(file_path)

    def load_examples(self, file_path):
        """Load request types and keyword mappings from a JSON file"""
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)["rules"]
            if not isinstance(data, list):  # Ensure JSON is a list of dictionaries
                raise ValueError("Invalid format: JSON should be a list of dictionaries.")
            return data

    def generate_prompt(self, email_text):
        """Generate a structured prompt with examples for few-shot classification"""
        prompt = f"""
        You are an expert email classifier for loan servicing. 
        Classify the given email into **one or more** of the predefined request types and sub-request types.

        ### **Available Categories & Keywords**
        {json.dumps(self.examples, indent=2)}
        
        ### **Email to Classify**
        "{email_text}"
        
        **Rules:**
        - Identify the correct **request type** and **sub-request type** based on keyword matching.
        - If multiple request types match, return all relevant categories with confidence score.
        - If no category fits, return `"No Match"` with confidence `20`.
        - Ensure output is **valid JSON only**.

        **Output Format:**
        ```json
        [
            {{"requestType": "<Request Type or 'No Match'>", "subRequestType": "<Sub-Request Type or 'No Match'>", "confidenceScore": Confidence Score (0-100)}}
        ]
        ```
        """
        return prompt

    def llm_classification(self, email_text):
        """Step 1: Use LLM to classify the email and adjust confidence score if no match is found."""
        prompt = self.generate_prompt(email_text)

        response = ollama.chat(
            model=self.model,
            messages=[{"role": "user", "content": prompt}],
            options={"num_predict": 150}
        )

        # Extract and clean response
        raw_output = response["message"]["content"].strip()

        # Remove unwanted Markdown (` ```json ` and ` ``` `)
        raw_output = raw_output.replace("```json", "").replace("```", "").strip()

        try:
            classification_result = json.loads(raw_output)

            # Ensure output is always a list
            if isinstance(classification_result, dict):
                classification_result = [classification_result]  

            return classification_result

        except json.JSONDecodeError:
            return [{"error": "Invalid JSON response", "raw_output": raw_output, "confidenceScore": 10}]

    def rule_based_classification(self, email_text):
        """Step 2: Verify LLM results using keyword-based rule matching."""
        matched_categories = []

        for request in self.examples:
            for sub_request in request["subRequestTypes"]:
                for keyword in sub_request["keywords"]:
                    # Use regex for whole-word matching
                    pattern = r"\b" + re.escape(keyword.lower()) + r"\b"
                    if re.search(pattern, email_text.lower()):
                        matched_categories.append({
                            "requestType": request["requestType"],
                            "subRequestType": sub_request["subRequestType"],
                            "confidenceScore": 95  # Rule-based matches are high confidence
                        })

        return matched_categories if matched_categories else [{"requestType": "No Match", "subRequestType": "No Match", "confidenceScore": 20}]

    def verify_llm_with_rules(self, llm_result, rule_based_result):
        """Step 3: Compare LLM and rule-based classifications, adjust confidence if needed."""
        verified_results = []

        for llm_category in llm_result:
            match_found = False

            for rule_category in rule_based_result:
                #if llm_category["requestType"] == rule_category["requestType"]:
                if llm_category.get("requestType", "No Match") == rule_category.get("requestType", "No Match"):

                    # If LLM and rule-based results match, keep the higher confidence
                    verified_results.append(llm_category)
                    match_found = True
                    break  

            if not match_found:
                # Lower confidence if LLM classification doesn't match rule-based classification
                llm_category["confidenceScore"] = 20  
                verified_results.append(llm_category)

        return verified_results

    def classify_email(self, email_text, filepath):
        """Run LLM for email classification, verify with rule-based matching, and adjust confidence scores."""
        # Step 1: Get classification from LLM
        llm_result = self.llm_classification(email_text)

        # Step 2: Get rule-based classification
       # rule_based_result = self.rule_based_classification(email_text)

        # Step 3: Verify LLM output against rule-based results
       # verified_result = self.verify_llm_with_rules(llm_result, rule_based_result)

        # Append filename to each result
        for result in llm_result:
            result["filename"] = filepath
        
        return llm_result

