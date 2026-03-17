from pydantic import BaseModel, HttpUrl, Field
from typing import List, Optional
from datetime import datetime
import hashlib

class JobSource(BaseModel):
    site_name: str
    url: HttpUrl
    crawled_at: datetime = Field(default_factory=datetime.now)

class JobSchema(BaseModel):
    job_title: str
    company_name: str
    location: Optional[str] = None
    description: str
    salary_raw: Optional[str] = None
    salary_min: Optional[float] = None
    salary_max: Optional[float] = None
    currency: str = "VND"
    posted_date: Optional[datetime] = None
    contract_type: Optional[str] = None
    experience_level: Optional[str] = None
    skills: List[str] = []
    
    def generate_dedup_hash(self) -> str:
        """Generates a hash based on normalized title, company, and description"""
        content = f"{self.company_name.lower()}|{self.job_title.lower()}|{self.description[:1000].lower()}"
        return hashlib.md5(content.encode()).hexdigest()

class ProcessedJob(JobSchema):
    id: Optional[str] = None
    dedup_hash: Optional[str] = None
    sources: List[JobSource] = []
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
