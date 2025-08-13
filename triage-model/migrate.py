from pymongo import MongoClient
from datetime import datetime
import uuid
from collections import defaultdict
from bson import ObjectId
from tqdm import tqdm
import traceback

# MongoDB Connection
client = MongoClient('mongodb+srv://crawcoadmin:Chang3M3Now!@cosmon-openai-uk-lw.global.mongocluster.cosmos.azure.com/?tls=true&authMechanism=SCRAM-SHA-256&retrywrites=false&maxIdleTimeMS=120000')
db = client['CrawcoCovAIDB']  # Change to your DB name

# Collections
documents_col = db['Documents']
policies_col = db['Policies']
declarations_col = db['Declaration']
clients_col = db['Clients']
prompt_col = db['DefaultPrompt']
msa_documents_col = db['master_lease_documents']

documents_revamp_col = db['DocumentsRevampMigrate']
processed_json_revamp_col = db['ProcessedJsonRevampMigrate']
merged_document_revamp_col = db['MergedDocumentRevampMigrate']

split_docs_grouped = defaultdict(list)
Exceptions = {
    "Documents": [],
    "Declaration": [],
    "Merged": [],
    "Chiled_documents": {
        "applied": [],
        "base_endo": [],
        "others": []
    }
}
# Utility Functions
def generate_json_id():
    return str(uuid.uuid4())

def get_declaration_unique_id(document_id):
    declaration = declarations_col.find_one({"DeclarationDocumentID": document_id})
    if declaration:
        return declaration.get("DeclarationNumber")
    return None

def get_client_config_id(document_client_name):
    client = clients_col.find_one({"ClientName": document_client_name})
    if client:
        return str(client.get("_id"))
    return None

def save_json_and_get_id(json_data):
    if json_data:
        json_id = generate_json_id()
        processed_json_revamp_col.insert_one({
            "jsonId": json_id,
            "JsonData": json_data,
            "CreatedDate": datetime.utcnow()
        })
        return json_id
    return None


def migrate_documents():
    print("Migrating Documents collection")
    # documents = list(documents_col.find({}))
    failed_ids = []
    for document in tqdm(documents):
        try:
        # Step 1: Insert into DocumentsRevamp
            doc_type = document.get("DocumentType")
            client_name = document.get("CarrierName")
            policy_name = None
            
            config_id = get_client_config_id(client_name) if client_name else None
            
            unique_id = get_declaration_unique_id(document['ID'])
    
            main_json_id = save_json_and_get_id(document.get('JsonData', {}))
            
            prompt_obj = prompt_col.find_one({"CarrierName": client_name, "PolicyName": policy_name})

            prompt_id = prompt_obj.get("PromptID") if prompt_obj else None

            if "base" in doc_type.lower():
                policy_obj = policies_col.find_one({"BasePolicyDocumentID": document.get("ID")})
                policy_name = policy_obj.get("PolicyName") if policy_obj else None
            elif "endo" in doc_type.lower():
                policy_obj = policies_col.find_one({"EndorsementDocumentID": document.get("ID")})
                policy_name = policy_obj.get("PolicyName") if policy_obj else None
            elif "dec" in doc_type.lower():
                policy_obj = declarations_col.find_one({"DeclarationDocumentID": document.get("ID")})
                policy_name = policy_obj.get("PolicyName") if policy_obj else None

        
            new_doc = {
                "useCase": "policy lookup",
                "userId": document.get('UserID'),
                "uniqueId": unique_id,
                "documentId": document.get('ID'),
                "prompt": {
                    "qnAPrompId": prompt_id,
                    "globalComparisonPromptId": None,
                    "globalQnAPromptId": None,
                    "comparsionPromptId": None
                },
                "documentName": document.get('DocumentName'),
                "clientName": document.get('CarrierName'),
                "clientID": config_id,
                "documentTag": document.get('DocumentType'),
                "documentSubTag": policy_name,
                "filePath": document.get('DocumentPath'),
                "declarationType": None,
                "processStatus": document.get('Status'),
                "adobeExtractPath": document.get('AdobeExtractPath'),
                "uploadDate": document.get('UploadedAt', None),
                "processedDate": document.get('ProcessedAt', None),
                "policyStatus": None,
                "retryCount": document.get('RetryCount', 0),
                "failure": {
                    "reason": document.get('FailureReason'),
                    "code": document.get('FailureStatusCode', [])
                },
                "formNumber": {
                    "formNumber": document.get('FormNumber'),
                    "normalizedFormNumber": document.get('NormalizedFormNumber')
                },
                "extractedFormNumbers": [],
                "isDelete": False,
                "isActive": True,
                "jsonId": main_json_id,
                "metaData": {},
                "childDocumentIds": []
            }
    
            documents_revamp_col.insert_one(new_doc)
        except Exception as e:
            print(e)
            failed_ids.append(document['ID'])
            # print(f"Exception occured while creating entry in DocumentsRevampMigrate for ID: {document['ID']}")
    Exceptions["Documents"] += failed_ids


def migrate_declarations():
    print("Starting Declaration Metadata Migration...")
    
    all_declarations = list(declarations_col.find({}))
    failed_ids = []
    for declaration in tqdm(all_declarations, desc="Migrating Declarations"):
        try:
            holder_name = declaration.get("HolderName")
            start_date = declaration.get("StartDate", None)
            expiry_date = declaration.get("ExpiryDate", None)
            declaration_number = declaration.get("DeclarationNumber")
            version = declaration.get("Version")
            processed_format = declaration.get("ProcessedFormat")
            declaration_doc_ids = declaration.get("DeclarationDocumentID", [])
            # print(declaration_doc_ids)
    
            metadata = {
                "holderName": holder_name,
                "startDate": start_date,
                "endDate": expiry_date,
                "version": version,
                "cancellationDate": None
            }
            try:
                for base_id in declaration_doc_ids:
                    update_result = documents_revamp_col.update_many(
                        {"documentId": base_id},
                        {"$set": {"metaData": metadata,
                                 "uniqueId": declaration_number}}
                    )
            except Exception as e:
                failed_ids.append(declaration_doc_ids)
                err_msg = ''.join(traceback.format_exception(None, e, e.__traceback__))
        except Exception as e:
            failed_ids += declaration_doc_ids
            err_msg = ''.join(traceback.format_exception(None, e, e.__traceback__))
            # print(f"Exception occured while updating metadata from declaration for ID: {declaration['DeclarationDocumentID']}", err_msg)
    Exceptions["Declaration"] += failed_ids
        # break

def migrate_merged_col():
    print("Migrating Merged col collection")
    # all_documents = doc_data
    failed_ids = []
    # Group split documents by base ID
    print("finding merged documents")
    for doc in tqdm(documents):
        doc_id = doc.get("ID", "")
        if "_" in doc_id:
            base_id = doc_id.split("_")[0]
            split_docs_grouped[base_id].append(doc)

    # Process each merged document group
    print("processing merged documents")
    for base_id, split_docs in tqdm(split_docs_grouped.items()):
        # print(f"base_id: {base_id}")
        try:
        # Find the base merged document
            merged_doc = documents_col.find_one({"ID": base_id})
            if not merged_doc:
                continue  # Skip if original merged doc not found
    
            # Construct splitDocumentList
            split_document_list = []
            for split_doc in split_docs:
                try:
                    split_document_list.append({
                        "documentId": split_doc.get("ID"),
                        "processStatus": split_doc.get("Status", None),
                        "isActive": True,
                        "filePath": split_doc.get("DocumentPath", None),
                        "documentTag": split_doc.get("DocumentType", None),
                        "formNumber": {
                            "formNumber": split_doc.get("FormNumber"),
                            "normalizedFormNumber": split_doc.get("NormalizedFormNumber")
                        }
                    })
                except Exception as e:
                    failed_ids.append(split_doc.get('ID'))
                    err_msg = ''.join(traceback.format_exception(None, e, e.__traceback__))
                    # print(f"Exception occured while creating the object for split_doc for merged for ID: {split_doc.get('ID')}", err_msg)
    
            # Create merged revamp entry
            merged_revamp_doc = {
                "uniqueId": None,
                "documentId": base_id,
                "userId": merged_doc.get("UserID"),
                "clientName": merged_doc.get("CarrierName"),
                "documentTag": "Merged Document",
                "documentSubTag": None,
                "documentName": merged_doc.get("DocumentName"),
                "filePath": merged_doc.get("DocumentPath"),
                "adobeExtractPath": merged_doc.get("AdobeExtractPath"),
                "uploadDate": merged_doc.get("UploadedAt") if merged_doc.get("UploadedAt") else None,
                "processedDate": merged_doc.get("ProcessedAt") if merged_doc.get("ProcessedAt") else None,
                "processStatus": merged_doc.get("Status", None),
                "retryCount": merged_doc.get("RetryCount", 0),
                "failure": {
                    "reason": merged_doc.get("FailureReason"),
                    "code": merged_doc.get("FailureStatusCode", [])
                },
                "isActive": True,
                "splitIndices": {},  # You can customize this if split index logic is needed
                "splitDocumentList": split_document_list,
                "useCase": "policy lookup"  # Replace with actual use case name if dynamic
            }
    
            # Insert into MergedRevamp collection
            merged_document_revamp_col.insert_one(merged_revamp_doc)
        except Exception as e:
            failed_ids.append(base_id)
            err_msg = ''.join(traceback.format_exception(None, e, e.__traceback__))
            # print(f"Exception occured while updating the splitDocumentList for ID: {base_id}", err_msg)
    Exceptions["Merged"] += failed_ids
        # break

            
def update_child_documents():
    print("Migrating update child documents collection")
    policies = list(policies_col.find({}))
    failed_ids = []
    for policy in tqdm(policies):
        try:
            carrier_name = policy.get('CarrierName')
            policy_name = policy.get('PolicyName')
    
            base_doc_id = policy.get('BasePolicyDocumentID')
            endorsement_doc_ids = policy.get('EndorsementDocumentID', [])
    
            declaration = declarations_col.find_one({
                "CarrierName": carrier_name,
                "PolicyName": policy_name
            })
    
            if not declaration:
                continue
            applied_endorsement_ids = declaration.get('AppliedEndorsement')
            
            declaration_doc_id = declaration.get('DeclarationDocumentID')
            if not declaration_doc_id:
                continue
            # print(declaration_doc_id)
    
            # Step 3: Prepare childDocumentIds from AppliedEndorsement
            child_documents = []
            try:
                for doc_id in applied_endorsement_ids:
                    doc_revamp = documents_revamp_col.find_one({"documentId": doc_id})
                    if doc_revamp:
                        child_documents.append({
                            "documentId": doc_revamp['documentId'],
                            "isActive": True,
                            "documentTag": doc_revamp.get('documentTag'),
                            "filePath": doc_revamp.get('filePath'),
                            "jsonId": doc_revamp.get('jsonId')
                        })
                    else:
                        failed_ids.append(doc_id)
            except:
                failed_ids += declaration_doc_id
                Exceptions["Chiled_documents"]["applied"].append(declaration_doc_id)
                # print(f"Empty applied endorsement for {declaration_doc_id}")
            # Step 4: Prepare extractedFormNumbers from BasePolicyDocumentID and EndorsementDocumentID
            form_doc_ids = []
            if base_doc_id:
                form_doc_ids.append(base_doc_id)
            if endorsement_doc_ids:
                form_doc_ids.extend(endorsement_doc_ids)

            # print(f"form_doc_ids: {form_doc_ids}")
            
            extracted_form_numbers = []
            try:
                for doc_id in form_doc_ids:
                    doc_revamp = documents_revamp_col.find_one({"documentId": doc_id})
                    if doc_revamp:
                        form_info = doc_revamp.get('formNumber', {})
                        if form_info and form_info.get('formNumber'):
                            extracted_form_numbers.append({
                                "formNumber": form_info.get('formNumber'),
                                "normalizedFormNumber": form_info.get('normalizedFormNumber')
                            })
                        else:
                            failed_ids.append(doc_id)
                    else:
                        failed_ids.append(doc_id)
                            # print(f"extracted_form_numbers: {extracted_form_numbers}")
            except:
                failed_ids += declaration_doc_id
                Exceptions["Chiled_documents"]["base_endo"].append(declaration_doc_id)
                # print(f"no base policy and endorsement for ID {declaration_doc_id}")
    
            # Step 5: Update Declaration Document in DocumentsRevamp
            for dec_id in declaration_doc_id:
                documents_revamp_col.update_one(
                    {"documentId": dec_id},
                    {
                        "$set": {
                            "childDocumentIds": child_documents,
                            "extractedFormNumbers": extracted_form_numbers
                        }
                    }
                )
        except Exception as e:
            failed_ids += declaration_doc_id
            err_msg = ''.join(traceback.format_exception(None, e, e.__traceback__))
            # print(f"Exception occured while updating the childDocumentIds and extractedFormNumbers for \nCarrier Name: {carrier_name}\nPolicy Name: {policy_name}", err_msg)
            Exceptions["Chiled_documents"]["others"].append(declaration_doc_id)

def migrate_msa_documents():
    print("Migrating Documents collection")
    msa_documents = list(msa_documents_col.find({}))
    failed_ids = []
    for document in tqdm(msa_documents):
        try:
            doc_type = document.get("DocumentType")
            client_name = document.get("CarrierName")
            policy_name = None
            config_id = get_client_config_id(client_name) if client_name else None
    
            main_json_id = save_json_and_get_id(document.get('JsonData', {}))
            
            prompt_obj = prompt_col.find_one({"CarrierName": client_name, "PolicyName": policy_name})

            prompt_id = prompt_obj.get("PromptID") if prompt_obj else None

        
            new_doc = {
                "useCase": "policy lookup",
                "userId": document.get('UserID'),
                "uniqueId": None,
                "documentId": document.get('ID'),
                "prompt": {
                    "qnAPrompId": prompt_id,
                    "globalComparisonPromptId": None,
                    "globalQnAPromptId": None,
                    "comparsionPromptId": None
                },
                "documentName": document.get('DocumentName'),
                "clientName": client_name,
                "clientID": config_id,
                "documentTag": doc_type,
                "documentSubTag": document.get("PolicyName"),
                "filePath": document.get('DocumentPath'),
                "declarationType": None,
                "processStatus": document.get('Status'),
                "adobeExtractPath": document.get('AdobeExtractPath'),
                "uploadDate": document.get('UploadedAt'),
                "processedDate": document.get('ProcessedAt'),
                "policyStatus": None,
                "retryCount": document.get('RetryCount', 0),
                "failure": {
                    "reason": document.get('FailureReason', None),
                    "code": document.get('FailureStatusCode', [])
                },
                "formNumber": {
                    "formNumber": document.get('FormNumber'),
                    "normalizedFormNumber": document.get('NormalizedFormNumber')
                },
                "extractedFormNumbers": [],
                "isDelete": False,
                "isActive": True,
                "jsonId": main_json_id,
                "metaData": {},
                "childDocumentIds": []
            }
            documents_revamp_col.insert_one(new_doc)
        except Exception as e:
            failed_ids.append(document['ID'])
            err_msg = ''.join(traceback.format_exception(None, e, e.__traceback__))
            print(f"Exception occured while creating entry in DocumentsRevampMigrate for ID: {document['ID']}", err_msg)
    Exceptions["Documents"] += failed_ids

# ---- Running ----
if __name__ == "__main__":
    migrate_documents()
    migrate_declarations()
    update_child_documents()
    migrate_merged_col()
    migrate_msa_documents()
    print("Migration and child document update completed successfully!")
