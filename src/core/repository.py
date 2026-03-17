from typing import List, Optional, Any, Dict
from ..models.orm_models import SiteExtractRule
import hashlib

class RuleRepository:
    """
    Repository Pattern: Tách biệt logic truy vấn Rule khỏi Business Logic.
    Giúp code dễ bảo trì và dễ viết Unit Test (Mocking).
    """
    async def get_active_rules(self, site_name: str) -> List[Dict[str, Any]]:
        """Lấy tất cả các luật đang hoạt động (verified) của một trang"""
        rules = await SiteExtractRule.filter(
            site_name=site_name, 
            status='verified'
        ).order_by('-version').values('field_name', 'selector', 'selector_type', 'version')
        
        # Chỉ lấy version mới nhất cho mỗi field
        latest = {}
        for r in rules:
            if r['field_name'] not in latest:
                latest[r['field_name']] = r
        return list(latest.values())

    async def create_candidate(self, site_name: str, field_name: str, selector: str, s_type: str, source: str):
        """Tạo một rule ứng viên mới (chờ xác thực)"""
        return await SiteExtractRule.create(
            site_name=site_name,
            field_name=field_name,
            selector=selector,
            selector_type=s_type,
            status='candidate',
            source=source
        )

class JobRepository:
    """Quản lý việc lưu trữ Job vào Database thông qua ORM"""
    from ..models.orm_models import Job, JobSource

    async def update_or_create_job(self, data: Dict[str, Any]) -> tuple:
        """Lưu Job và xử lý trùng lặp dựa trên mã băm nội dung"""
        # Tạo mã băm để nhận diện tin trùng (Deduplication)
        content_str = f"{data['company_name'].lower()}|{data['job_title'].lower()}"
        dedup_hash = hashlib.md5(content_str.encode()).hexdigest()

        job_obj, created = await self.Job.update_or_create(
            dedup_hash=dedup_hash,
            defaults={
                "job_title": data['job_title'],
                "company_name": data['company_name'],
                "description": data['description'],
            }
        )
        return job_obj, created

    async def add_source(self, job, site_name: str, url: str):
        """Lưu vết nguồn gốc của tin tuyển dụng"""
        return await self.JobSource.get_or_create(
            job=job,
            site_name=site_name,
            source_url=url
        )
