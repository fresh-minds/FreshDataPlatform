from datahub_client import DataHubService
import logging
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import TagPropertiesClass

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_governance_defs():
    dh = DataHubService()
    
    # In DataHub, we can create Tags
    tags = [
        ("PII", "Personally Identifiable Information"),
        ("CONFIDENTIAL", "Sensitive business data"),
        ("GDPR_EU", "Data subject to GDPR regulation")
    ]
    
    for tag_name, description in tags:
        try:
            urn = f"urn:li:tag:{tag_name}"
            mcp = MetadataChangeProposalWrapper(
                entityUrn=urn,
                aspect=TagPropertiesClass(name=tag_name, description=description)
            )
            dh.emitter.emit(mcp)
            logger.info(f"Tag {tag_name} created successfully in DataHub.")
        except Exception as e:
            logger.error(f"Failed to create tag {tag_name}: {e}")

if __name__ == "__main__":
    create_governance_defs()
