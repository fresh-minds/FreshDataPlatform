import os
import logging
from urllib.parse import urlparse

from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import DatasetPropertiesClass

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataHubService:
    def __init__(self, host=None, port=None, token=None):
        gms_url = os.getenv("DATAHUB_GMS_URL") or os.getenv("DATAHUB_REST_URL")

        if host and port:
            self.host = host
            self.port = str(port)
            self.gms_url = f"http://{self.host}:{self.port}"
        elif gms_url:
            parsed = urlparse(gms_url)
            self.host = parsed.hostname or "localhost"
            self.port = str(parsed.port or 8080)
            self.gms_url = f"{parsed.scheme or 'http'}://{self.host}:{self.port}"
        else:
            self.host = host or os.getenv("DATAHUB_GMS_HOST", "localhost")
            # Local compose exposes GMS on 8081 externally; Airflow internal containers can override with env.
            self.port = str(port or os.getenv("DATAHUB_GMS_PORT", "8081"))
            self.gms_url = f"http://{self.host}:{self.port}"

        self.emitter = DatahubRestEmitter(self.gms_url, token=token)
        
        # Test connection
        try:
            self.emitter.test_connection()
            logger.info(f"Connected to DataHub at {self.gms_url}")
        except Exception as e:
            logger.warning(f"Could not connect to DataHub at {self.gms_url}: {e}")

    def emit_dataset_properties(self, dataset_urn, properties, description=None):
        """
        Emit properties for a dataset.
        dataset_urn: e.g. "urn:li:dataset:(urn:li:dataPlatform:postgres,open_data_platform_dw.public.job_market_nl,PROD)"
        """
        try:
            dataset_properties = DatasetPropertiesClass(
                customProperties=properties,
                description=description
            )
            
            mcp = MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=dataset_properties
            )
            
            self.emitter.emit(mcp)
            logger.info(f"Emitted properties for {dataset_urn}")
        except Exception as e:
            logger.error(f"Failed to emit DataHub properties: {e}")
            raise e

    def emit_lineage(self, upstream_urns, downstream_urn):
        """
        Simplified lineage emission.
        """
        # Logic for lineage emission using UpstreamLineageClass
        from datahub.metadata.schema_classes import UpstreamLineageClass, UpstreamClass, DatasetLineageTypeClass
        
        try:
            upstreams = [
                UpstreamClass(dataset=urn, type=DatasetLineageTypeClass.TRANSFORMED)
                for urn in upstream_urns
            ]
            lineage_aspect = UpstreamLineageClass(upstreams=upstreams)
            
            mcp = MetadataChangeProposalWrapper(
                entityUrn=downstream_urn,
                aspect=lineage_aspect
            )
            
            self.emitter.emit(mcp)
            logger.info(f"Emitted lineage: {upstream_urns} -> {downstream_urn}")
        except Exception as e:
            logger.error(f"Failed to emit DataHub lineage: {e}")

if __name__ == "__main__":
    # Test connection
    service = DataHubService()
    print("DataHub Client Initialized")
