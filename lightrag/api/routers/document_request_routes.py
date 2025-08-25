"""
Document Request Webhook System

This module handles document requests from the frontend and completion notifications from n8n,
enabling real-time status updates and document downloads.
"""

import asyncio
import json
import traceback
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Literal
from pathlib import Path
from io import BytesIO

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, File, UploadFile
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field, field_validator

from lightrag.utils import logger
from lightrag.api.utils_api import get_combined_auth_dependency
from lightrag.api.config import global_args
from lightrag.kg.shared_storage import get_namespace_data, get_storage_lock

# Shared storage namespace for document requests
DOCUMENT_REQUESTS_NS = "document_requests"

router = APIRouter(
    prefix="/webhook",
    tags=["document-requests"],
)

# Create combined auth dependency - will be set in create_document_request_routes function
combined_auth = None


class DocumentRequestWebhookRequest(BaseModel):
    """Request model for document request webhook (JSON format)
    
    This handles both initial requests (Type A) and completion notifications (Type B)
    """
    
    # Type A fields (initial request)
    requestId: Optional[str] = Field(
        default=None,
        description="Unique request ID (auto-generated if not provided)"
    )
    documentType: Optional[str] = Field(
        default=None,
        description="Type of document being requested"
    )
    parameters: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Additional parameters for document generation"
    )
    
    # Type B fields (completion notification)
    action: Optional[str] = Field(
        default=None,
        description="Action type (e.g., 'completed', 'failed')"
    )
    downloadUrl: Optional[str] = Field(
        default=None,
        description="URL where the completed document can be downloaded"
    )
    fileName: Optional[str] = Field(
        default=None,
        description="Name of the generated file"
    )
    fileSize: Optional[int] = Field(
        default=None,
        description="Size of the generated file in bytes"
    )
    errorMessage: Optional[str] = Field(
        default=None,
        description="Error message if processing failed"
    )
    
    @field_validator("requestId", mode="after")
    @classmethod
    def generate_request_id_if_missing(cls, request_id: Optional[str]) -> str:
        """Generate a request ID if not provided"""
        if not request_id:
            return str(uuid.uuid4())
        return request_id
    
    class Config:
        json_schema_extra = {
            "example": {
                "requestId": "req_12345",
                "documentType": "financial_report",
                "parameters": {
                    "year": 2024,
                    "quarter": "Q1"
                }
            }
        }


class DocumentRequestWebhookBinaryRequest(BaseModel):
    """Request model for document request webhook with binary file upload
    
    This handles binary file uploads with form data
    """
    
    requestId: Optional[str] = Field(
        default=None,
        description="Unique request ID (auto-generated if not provided)"
    )
    documentType: Optional[str] = Field(
        default=None,
        description="Type of document being uploaded"
    )
    parameters: Optional[str] = Field(
        default=None,
        description="Additional parameters as JSON string"
    )
    
    @field_validator("requestId", mode="after")
    @classmethod
    def generate_request_id_if_missing(cls, request_id: Optional[str]) -> str:
        """Generate a request ID if not provided"""
        if not request_id:
            return str(uuid.uuid4())
        return request_id


class DocumentRequestResponse(BaseModel):
    """Response model for document request webhook"""
    
    status: Literal["success", "error"] = Field(
        description="Status of the operation"
    )
    message: str = Field(
        description="Response message"
    )
    requestId: str = Field(
        description="Request ID for tracking"
    )
    
    class Config:
        json_schema_extra = {
            "example": {
                "status": "success",
                "message": "Document request created successfully",
                "requestId": "req_12345"
            }
        }


class DocumentRequestStatus(BaseModel):
    """Model for document request status"""
    
    requestId: str = Field(description="Unique request identifier")
    status: Literal["Requested", "Processing", "Ready", "Failed"] = Field(
        description="Current status of the request"
    )
    documentType: Optional[str] = Field(
        default=None,
        description="Type of document requested"
    )
    parameters: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Parameters used for document generation"
    )
    downloadUrl: Optional[str] = Field(
        default=None,
        description="URL for downloading the completed document"
    )
    fileName: Optional[str] = Field(
        default=None,
        description="Name of the generated file"
    )
    fileSize: Optional[int] = Field(
        default=None,
        description="Size of the file in bytes"
    )
    errorMessage: Optional[str] = Field(
        default=None,
        description="Error message if processing failed"
    )
    createdAt: str = Field(
        description="Creation timestamp (ISO format)"
    )
    updatedAt: str = Field(
        description="Last update timestamp (ISO format)"
    )
    file_content: Optional[bytes] = Field(
        default=None,
        description="Binary content of the file if stored directly"
    )
    
    class Config:
        json_schema_extra = {
            "example": {
                "requestId": "req_12345",
                "status": "Ready",
                "documentType": "financial_report",
                "parameters": {"year": 2024, "quarter": "Q1"},
                "downloadUrl": "https://example.com/downloads/report.pdf",
                "fileName": "financial_report_2024_Q1.pdf",
                "fileSize": 1024000,
                "createdAt": "2024-01-15T10:30:00Z",
                "updatedAt": "2024-01-15T10:35:00Z"
            }
        }


class DocumentRequestsResponse(BaseModel):
    """Response model for document requests list"""
    
    requests: List[DocumentRequestStatus] = Field(
        description="List of document requests"
    )
    total: int = Field(
        description="Total number of requests"
    )
    
    class Config:
        json_schema_extra = {
            "example": {
                "requests": [
                    {
                        "requestId": "req_12345",
                        "status": "Ready",
                        "documentType": "financial_report",
                        "downloadUrl": "https://example.com/downloads/report.pdf",
                        "fileName": "financial_report_2024_Q1.pdf",
                        "fileSize": 1024000,
                        "createdAt": "2024-01-15T10:30:00Z",
                        "updatedAt": "2024-01-15T10:35:00Z"
                    }
                ],
                "total": 1
            }
        }


def get_current_timestamp() -> str:
    """Get current timestamp in ISO format with timezone"""
    return datetime.now(timezone.utc).isoformat()


async def handle_binary_file_upload(
    file: UploadFile, 
    request_id: str, 
    document_type: str, 
    parameters: str
) -> DocumentRequestResponse:
    """Handle binary file upload via multipart/form-data"""
    try:
        db = await get_namespace_data(DOCUMENT_REQUESTS_NS)
        # Generate request ID if not provided
        if not request_id:
            request_id = str(uuid.uuid4())
        
        # Parse parameters if provided
        parsed_parameters = None
        if parameters:
            try:
                parsed_parameters = json.loads(parameters)
            except json.JSONDecodeError:
                logger.warning(f"Invalid JSON parameters: {parameters}")
        
        # Check if request already exists and create new document request with file info
        async with get_storage_lock():
            if request_id in db:
                raise HTTPException(
                    status_code=409,
                    detail=f"Document request with ID {request_id} already exists"
                )
            
            db[request_id] = {
                "requestId": request_id,
                "status": "Ready",  # File is immediately available
                "documentType": document_type or "uploaded_file",
                "parameters": parsed_parameters,
                "downloadUrl": None,  # File is stored in the request
                "fileName": file.filename,
                "fileSize": file.size,
                "errorMessage": None,
                "createdAt": get_current_timestamp(),
                "updatedAt": get_current_timestamp(),
                "file_content": await file.read()  # Store file content
            }
        
        logger.info(f"Binary file uploaded: {request_id}, filename: {file.filename}, size: {file.size}")
        
        return DocumentRequestResponse(
            status="success",
            message=f"Binary file uploaded successfully: {file.filename}",
            requestId=request_id
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error handling binary file upload: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


async def handle_json_request(request: DocumentRequestWebhookRequest) -> DocumentRequestResponse:
    """Handle JSON format request"""
    try:
        db = await get_namespace_data(DOCUMENT_REQUESTS_NS)
        request_id = request.requestId
        
        # Check if this is a completion notification (Type B)
        if request.action:
            # Type B: Completion notification
            async with get_storage_lock():
                if request_id not in db:
                    raise HTTPException(
                        status_code=404,
                        detail=f"Document request with ID {request_id} not found"
                    )
                
                # Update existing request
                db[request_id].update({
                    "status": "Ready" if request.action == "completed" else "Failed",
                    "downloadUrl": request.downloadUrl,
                    "fileName": request.fileName,
                    "fileSize": request.fileSize,
                    "errorMessage": request.errorMessage,
                    "updatedAt": get_current_timestamp()
                })
            
            status_message = "Document processing completed successfully"
            if request.action == "failed":
                status_message = f"Document processing failed: {request.errorMessage or 'Unknown error'}"
            
            logger.info(f"Document request {request_id} updated: {request.action}")
            
            return DocumentRequestResponse(
                status="success",
                message=status_message,
                requestId=request_id
            )
        
        else:
            # Type A: Initial request
            async with get_storage_lock():
                # Check if request already exists
                if request_id in db:
                    raise HTTPException(
                        status_code=409,
                        detail=f"Document request with ID {request_id} already exists"
                    )
                
                # Create new document request
                db[request_id] = {
                    "requestId": request_id,
                    "status": "Requested",
                    "documentType": request.documentType,
                    "parameters": request.parameters,
                    "downloadUrl": None,
                    "fileName": None,
                    "fileSize": None,
                    "errorMessage": None,
                    "createdAt": get_current_timestamp(),
                    "updatedAt": get_current_timestamp()
                }
            
            logger.info(f"New document request created: {request_id}")
            
            return DocumentRequestResponse(
                status="success",
                message="Document request created successfully",
                requestId=request_id
            )
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error handling JSON request: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


async def handle_form_completion_notification(
    request_id: str,
    action: str,
    download_url: str,
    file_name: str,
    file_size: int,
    error_message: str
) -> DocumentRequestResponse:
    """Handle completion notification via form data"""
    try:
        db = await get_namespace_data(DOCUMENT_REQUESTS_NS)
        if not request_id:
            raise HTTPException(status_code=400, detail="requestId is required")
        
        async with get_storage_lock():
            if request_id not in db:
                raise HTTPException(
                    status_code=404,
                    detail=f"Document request with ID {request_id} not found"
                )
            
            # Update existing request
            db[request_id].update({
                "status": "Ready" if action == "completed" else "Failed",
                "downloadUrl": download_url,
                "fileName": file_name,
                "fileSize": file_size,
                "errorMessage": error_message,
                "updatedAt": get_current_timestamp()
            })
        
        status_message = "Document processing completed successfully"
        if action == "failed":
            status_message = f"Document processing failed: {error_message or 'Unknown error'}"
        
        logger.info(f"Document request {request_id} updated via form: {action}")
        
        return DocumentRequestResponse(
            status="success",
            message=status_message,
            requestId=request_id
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error handling form completion notification: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


async def handle_form_initial_request(
    request_id: str,
    document_type: str,
    parameters: str
) -> DocumentRequestResponse:
    """Handle initial request via form data"""
    try:
        db = await get_namespace_data(DOCUMENT_REQUESTS_NS)
        # Generate request ID if not provided
        if not request_id:
            request_id = str(uuid.uuid4())
        
        # Parse parameters if provided
        parsed_parameters = None
        if parameters:
            try:
                parsed_parameters = json.loads(parameters)
            except json.JSONDecodeError:
                logger.warning(f"Invalid JSON parameters: {parameters}")
        
        # Check if request already exists and create new document request
        async with get_storage_lock():
            if request_id in db:
                raise HTTPException(
                    status_code=409,
                    detail=f"Document request with ID {request_id} already exists"
                )
            
            db[request_id] = {
                "requestId": request_id,
                "status": "Requested",
                "documentType": document_type,
                "parameters": parsed_parameters,
                "downloadUrl": None,
                "fileName": None,
                "fileSize": None,
                "errorMessage": None,
                "createdAt": get_current_timestamp(),
                "updatedAt": get_current_timestamp()
            }
        
        logger.info(f"New document request created via form: {request_id}")
        
        return DocumentRequestResponse(
            status="success",
            message="Document request created successfully",
            requestId=request_id
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error handling form initial request: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


def create_document_request_routes(api_key: Optional[str] = None):
    """Create document request routes"""
    global combined_auth
    combined_auth = get_combined_auth_dependency(api_key)
    
    @router.post(
        "/426951f9-1936-44c3-83ae-8f52f0508acf",
        response_model=DocumentRequestResponse,
        dependencies=[Depends(combined_auth)],
    )
    async def document_request_webhook(
        request: DocumentRequestWebhookRequest = None,
        file: UploadFile = File(None),
        requestId: str = None,
        documentType: str = None,
        parameters: str = None,
        action: str = None,
        downloadUrl: str = None,
        fileName: str = None,
        fileSize: int = None,
        errorMessage: str = None
    ):
        """
        Webhook endpoint for document requests and completion notifications.
        
        This endpoint handles both JSON and multipart/form-data requests:
        
        JSON Format:
        - Type A (Initial Request): No 'action' field present
        - Type B (Completion Notification): Has 'action' field present
        
        Multipart/Form-Data Format:
        - Binary file upload with form fields
        - Creates document request with uploaded file
        
        Args:
            request: JSON request body (optional)
            file: Uploaded binary file (optional)
            requestId: Request ID from form data
            documentType: Document type from form data
            parameters: Parameters as JSON string from form data
            action: Action type from form data
            downloadUrl: Download URL from form data
            fileName: File name from form data
            fileSize: File size from form data
            errorMessage: Error message from form data
        
        Returns:
            DocumentRequestResponse: Response with status and request ID
        """
        try:
            # Handle form data with action (completion notification) - CHECK THIS FIRST
            if action is not None:
                return await handle_form_completion_notification(
                    requestId, action, downloadUrl, fileName, fileSize, errorMessage
                )
            
            # Handle multipart/form-data (binary file upload)
            if file is not None:
                return await handle_binary_file_upload(
                    file, requestId, documentType, parameters
                )
            
            # Handle JSON request
            if request is not None:
                return await handle_json_request(request)
            
            # Handle form data without action (initial request)
            return await handle_form_initial_request(requestId, documentType, parameters)
                
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error in document request webhook: {str(e)}")
            logger.error(traceback.format_exc())
            raise HTTPException(status_code=500, detail=str(e))
    
    @router.get(
        "/api/document-requests",
        response_model=DocumentRequestsResponse,
        dependencies=[Depends(combined_auth)],
    )
    async def get_document_requests():
        """
        Get all document requests for frontend display.
        
        Returns:
            DocumentRequestsResponse: List of all document requests ordered by creation date
        """
        try:
            db = await get_namespace_data(DOCUMENT_REQUESTS_NS)
            # Get all requests and sort by created_at DESC
            requests = list(db.values())
            requests.sort(key=lambda x: x["createdAt"], reverse=True)
            
            # Convert to DocumentRequestStatus objects
            document_requests = [
                DocumentRequestStatus(**request) for request in requests
            ]
            
            return DocumentRequestsResponse(
                requests=document_requests,
                total=len(document_requests)
            )
            
        except Exception as e:
            logger.error(f"Error getting document requests: {str(e)}")
            logger.error(traceback.format_exc())
            raise HTTPException(status_code=500, detail=str(e))
    
    @router.get(
        "/api/document-requests/{request_id}",
        response_model=DocumentRequestStatus,
        dependencies=[Depends(combined_auth)],
    )
    async def get_document_request(request_id: str):
        """
        Get a specific document request by ID.
        
        Args:
            request_id: The unique request identifier
        
        Returns:
            DocumentRequestStatus: The document request details
        """
        try:
            db = await get_namespace_data(DOCUMENT_REQUESTS_NS)
            if request_id not in db:
                raise HTTPException(
                    status_code=404,
                    detail=f"Document request with ID {request_id} not found"
                )
            
            return DocumentRequestStatus(**db[request_id])
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error getting document request {request_id}: {str(e)}")
            logger.error(traceback.format_exc())
            raise HTTPException(status_code=500, detail=str(e))
    
    @router.get(
        "/api/document-requests/{request_id}/download",
        dependencies=[Depends(combined_auth)],
    )
    async def download_document_file(request_id: str):
        """
        Download the binary file content for a specific document request.
        
        Args:
            request_id: The unique request identifier
        
        Returns:
            StreamingResponse: The binary file content with appropriate headers
        """
        try:
            db = await get_namespace_data(DOCUMENT_REQUESTS_NS)
            if request_id not in db:
                raise HTTPException(
                    status_code=404,
                    detail=f"Document request with ID {request_id} not found"
                )
            
            request_data = db[request_id]
            
            # Check if file content exists
            if "file_content" not in request_data or request_data["file_content"] is None:
                raise HTTPException(
                    status_code=404,
                    detail=f"No file content available for request {request_id}"
                )
            
            # Check if status is Ready
            if request_data["status"] != "Ready":
                raise HTTPException(
                    status_code=400,
                    detail=f"Document is not ready for download. Current status: {request_data['status']}"
                )
            
            file_content = request_data["file_content"]
            file_name = request_data.get("fileName", f"document_{request_id}")
            
            # Determine content type based on file extension
            content_type = "application/octet-stream"
            if file_name.lower().endswith(('.pdf',)):
                content_type = "application/pdf"
            elif file_name.lower().endswith(('.doc', '.docx')):
                content_type = "application/msword"
            elif file_name.lower().endswith(('.xls', '.xlsx')):
                content_type = "application/vnd.ms-excel"
            elif file_name.lower().endswith(('.txt',)):
                content_type = "text/plain"
            elif file_name.lower().endswith(('.csv',)):
                content_type = "text/csv"
            
            return StreamingResponse(
                BytesIO(file_content),
                media_type=content_type,
                headers={
                    "Content-Disposition": f"attachment; filename={file_name}",
                    "Content-Length": str(len(file_content))
                }
            )
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error downloading file for request {request_id}: {str(e)}")
            logger.error(traceback.format_exc())
            raise HTTPException(status_code=500, detail=str(e))
    
    @router.delete(
        "/api/document-requests/{request_id}",
        dependencies=[Depends(combined_auth)],
    )
    async def delete_document_request(request_id: str):
        """
        Delete a document request by ID.
        
        Args:
            request_id: The unique request identifier
        
        Returns:
            Success message
        """
        try:
            db = await get_namespace_data(DOCUMENT_REQUESTS_NS)
            async with get_storage_lock():
                if request_id not in db:
                    raise HTTPException(
                        status_code=404,
                        detail=f"Document request with ID {request_id} not found"
                    )
                
                del db[request_id]
            logger.info(f"Document request deleted: {request_id}")
            
            return {"status": "success", "message": f"Document request {request_id} deleted successfully"}
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error deleting document request {request_id}: {str(e)}")
            logger.error(traceback.format_exc())
            raise HTTPException(status_code=500, detail=str(e))
    
    return router 