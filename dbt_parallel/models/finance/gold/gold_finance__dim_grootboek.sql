{{ config(alias='dim_grootboek') }}

select distinct
    grootboek_id,
    grootboek_nummer,
    grootboek_factuur_nummer,
    grootboek_document_nummer,
    grootboek_omschrijving,
    grootboekrekening_code,
    grootboekrekening_omschrijving,
    administratie_code,
    level0_classification_code,
    level0_description,
    level1_classification_code,
    level1_description
from {{ ref('silver_finance__financialtransactionlines_hierarchy') }}
