from ...models.orm_models import SiteExtractRule
from ...core.repository import RuleRepository
from ...utils.logger import logger

async def seed_all_rules():
    """Tập trung logic Seeding vào một hàm duy nhất để làm sạch main.py"""
    rule_repo = RuleRepository()
    
    # LinkedIn
    await rule_repo.reset_rules("linkedin")
    
    linkedin_rules = [
        {"field_name": "job_title", "selector": ".top-card-layout__title"},
        {"field_name": "company_name", "selector": ".topcard__org-name-link"},
        {"field_name": "description", "selector": ".show-more-less-html__markup"},
        {"field_name": "location", "selector": ".topcard__flavor--bullet"},
        {"field_name": "posted_date", "selector": ".posted-time-ago__text"},
        {"field_name": "industry", "selector": ".description__job-criteria-list li:nth-child(4) .description__job-criteria-text"},
        {"field_name": "job_function", "selector": ".description__job-criteria-list li:nth-child(3) .description__job-criteria-text"},
    ]
    
    for r in linkedin_rules:
        await SiteExtractRule.create(
            site_name="linkedin",
            selector_type="css",
            **r
        )
    
    logger.info("db.seeding.completed", site="linkedin")
