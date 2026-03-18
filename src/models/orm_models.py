from tortoise import fields, models
from tortoise.contrib.pydantic import pydantic_model_creator

class SiteExtractRule(models.Model):
    id = fields.IntField(pk=True)
    site_name = fields.CharField(max_length=100)
    field_name = fields.CharField(max_length=100)
    selector = fields.TextField()
    selector_type = fields.CharField(max_length=10, default="css")
    version = fields.IntField(default=1)
    status = fields.CharField(max_length=20, default="verified") # verified, candidate, deprecated
    confidence = fields.FloatField(null=True)
    source = fields.CharField(max_length=20, null=True) # manual, ai_recovery
    last_verified = fields.DatetimeField(null=True)
    fail_count = fields.IntField(default=0)
    created_at = fields.DatetimeField(auto_now_add=True)

    class Meta:
        table = "site_extract_rules"
        schema = "jobranking"
        unique_together = (("site_name", "field_name", "version"),)

class Job(models.Model):
    id = fields.UUIDField(pk=True)
    job_title = fields.TextField()
    company_name = fields.TextField()
    location = fields.TextField(null=True)
    description = fields.TextField(null=True)
    salary_raw = fields.TextField(null=True)
    salary_min = fields.FloatField(null=True)
    salary_max = fields.FloatField(null=True)
    currency = fields.CharField(max_length=10, default="VND")
    posted_date = fields.DatetimeField(null=True)
    contract_type = fields.TextField(null=True)
    experience_level = fields.TextField(null=True)
    industry = fields.TextField(null=True)
    job_function = fields.TextField(null=True)
    skills = fields.JSONField(default=[]) # Tortoise JSONField for arrays
    dedup_hash = fields.CharField(max_length=64, unique=True, index=True)
    created_at = fields.DatetimeField(auto_now_add=True)
    updated_at = fields.DatetimeField(auto_now=True)

    class Meta:
        table = "jobs"
        schema = "jobranking"

class JobSource(models.Model):
    id = fields.IntField(pk=True)
    job = fields.ForeignKeyField("models.Job", related_name="sources")
    site_name = fields.CharField(max_length=100)
    source_url = fields.CharField(max_length=500, unique=True)
    crawled_at = fields.DatetimeField(auto_now_add=True)

    class Meta:
        table = "job_sources"
        schema = "jobranking"
