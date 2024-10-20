unique_products = """
SELECT 
    produto,
    all_models.model,
    ARRAY_AGG(all_models.id) AS ids
FROM (
    SELECT 'notebook' AS produto, dgn.id, dgn.model
    FROM lakehouse.d_gold_notebooks dgn
    WHERE dgn.model IS NOT NULL AND dgn.model <> ''  
    UNION ALL
    SELECT 'smartphone' AS produto, dgs.id, dgs.model
    FROM lakehouse.d_gold_smartphone dgs
    WHERE dgs.model IS NOT NULL AND dgs.model <> ''  
    UNION ALL
    SELECT 'tv' AS produto, dgtv.id, dgtv.model
    FROM lakehouse.d_gold_tv dgtv
    WHERE dgtv.model IS NOT NULL AND dgtv.model <> ''  
    UNION ALL
    SELECT 'tablets' AS produto, dgt.id, dgt.model
    FROM lakehouse.d_gold_tablets dgt
    WHERE dgt.model IS NOT NULL AND dgt.model <> ''  
    UNION ALL
    SELECT 'smartwatch' AS produto, dgs2.id, dgs2.model
    FROM lakehouse.d_gold_smartwatch dgs2
    WHERE dgs2.model IS NOT NULL AND dgs2.model <> ''  
) AS all_models
GROUP BY produto, all_models.model 
ORDER BY produto, all_models.model
OFFSET 1;  
"""