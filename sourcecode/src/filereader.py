import os
import json
import chardet
import extract_msg
import time
from email import policy
from email.parser import BytesParser
from bs4 import BeautifulSoup
from docx import Document
from multiprocessing import Pool

def read_txt(file_path):
    """Reads .txt files efficiently with encoding detection."""
    with open(file_path, "rb") as file:
        raw_data = file.read()
        encoding = chardet.detect(raw_data)["encoding"] or "utf-8"
    
    with open(file_path, "r", encoding=encoding, errors="replace") as file:
        return file.read()

def read_docx(file_path):
    """Reads .docx files efficiently."""
    try:
        doc = Document(file_path)
        return "\n".join([para.text for para in doc.paragraphs if para.text.strip()])
    except Exception as e:
        return f"Error reading .docx file: {e}"

def extract_attachments(msg):
    """Extracts attachment text from .msg or .eml files."""
    attachment_texts = []
    temp_folder = "./resources/temp_attachments"

    os.makedirs(temp_folder, exist_ok=True)  #Create temp folder if missing

    for attachment in msg.attachments():
        if attachment.longFilename.endswith(".txt") or attachment.longFilename.endswith(".docx"):
            attachment_path = os.path.join(temp_folder, attachment.longFilename)
            attachment.save(customPath=attachment_path)
            
            if attachment.longFilename.endswith(".txt"):
                attachment_texts.append(read_txt(attachment_path))
            elif attachment.longFilename.endswith(".docx"):
                attachment_texts.append(read_docx(attachment_path))
            
            os.remove(attachment_path)  # Delete temp file after processing

    return "\n".join(attachment_texts)

def read_eml(file_path):
    """Reads .eml files and extracts both email body and attachment content."""
    try:
        with open(file_path, "rb") as file:
            msg = BytesParser(policy=policy.default).parse(file)

        subject = msg["subject"]
        sender = msg["from"]
        recipient = msg["to"]

        body = ""
        for part in msg.walk():
            if part.get_content_type() == "text/plain":
                body = part.get_payload(decode=True).decode(part.get_content_charset(), errors="ignore")
            elif part.get_content_type() == "text/html":
                html_body = part.get_payload(decode=True).decode(part.get_content_charset(), errors="ignore")
                body = BeautifulSoup(html_body, "html.parser").get_text()

        attachments = []
        for part in msg.iter_attachments():
            filename = part.get_filename()
            if filename:
                attachments.append(filename)

        return f"Subject: {subject}\nFrom: {sender}\nTo: {recipient}\n\n{body}\n\nAttachments: {', '.join(attachments)}"

    except Exception as e:
        return f"Error reading .eml file: {e}"

def read_msg(file_path):
    """Reads .msg files and extracts email body and attachment content."""
    try:
        msg = extract_msg.Message(file_path)
        body = msg.body
        attachments = extract_attachments(msg)

        return f"Subject: {msg.subject}\nFrom: {msg.sender}\nTo: {msg.to}\n\n{body}\n\nAttachments:\n{attachments}"
    
    except Exception as e:
        return f"Error reading .msg file: {e}"

def read_email_file(file_path):
    """Determines file type and reads accordingly."""
    file_ext = os.path.splitext(file_path)[1].lower()

    if file_ext == ".txt":
        return read_txt(file_path)
    elif file_ext == ".docx":
        return read_docx(file_path)
    elif file_ext == ".eml":
        return read_eml(file_path)
    elif file_ext == ".msg":
        return read_msg(file_path)
    else:
        return f"Unsupported file format: {file_ext}"

def read_all_emails_in_folder(folder_path):
    """Reads all supported email files in a folder using multiprocessing."""
    if not os.path.exists(folder_path):
        return {"error": "Folder does not exist."}

    email_data = []
    files = [
        os.path.join(folder_path, file)
        for file in os.listdir(folder_path)
        if file.lower().endswith((".txt", ".docx", ".eml", ".msg"))
    ]

    start_time = time.time()

    # Use multiprocessing for faster execution
    with Pool(processes=4) as pool:
        results = pool.map(read_email_file, files)

    for file_path, content in zip(files, results):
        email_data.append({"file_name": os.path.basename(file_path), "content": content})

    end_time = time.time()
    print(f"Reading completed in {end_time - start_time:.2f} seconds")

    return email_data  # Returns a list instead of JSON string
