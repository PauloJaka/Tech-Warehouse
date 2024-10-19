SELECT COUNT(DISTINCT title) AS num_produtos
FROM (
    SELECT dgn.title FROM d_gold_notebooks dgn
    UNION ALL
    SELECT dgs.title FROM d_gold_smartphone dgs
    UNION ALL
    SELECT dgtv.title FROM d_gold_tv dgtv
    UNION ALL
    SELECT dgt.title FROM d_gold_tablets dgt
     UNION ALL
    SELECT dgs2.title FROM d_gold_smartwatch dgs2
) AS all_titles;

SELECT 
    produto,
    COUNT(DISTINCT title) AS num_produtos,
    website
FROM (
    SELECT 'notebooks' AS produto, dgn.title, fg.website FROM d_gold_notebooks dgn
    join f_gold fg on dgn.id = fg.id
    UNION ALL
    SELECT 'smartphones' AS produto, dgs.title, fg.website FROM d_gold_smartphone dgs
     join f_gold fg on dgs.id = fg.id
    UNION ALL
    SELECT 'tv' AS produto, dgtv.title, fg.website FROM d_gold_tv dgtv
    join f_gold fg on dgtv.id = fg.id
    UNION ALL
    SELECT 'tablets' AS produto, dgt.title, fg.website FROM d_gold_tablets dgt
       join f_gold fg on dgt.id = fg.id
    UNION ALL
    SELECT 'smartwatchs' AS produto, dgs2.title, fg.website FROM d_gold_smartwatch dgs2 
       join f_gold fg on dgs2.id = fg.id
) AS all_titles
GROUP BY produto, website
order by website;

SELECT 
    produto,
    COUNT(DISTINCT title) AS num_produtos
FROM (
    SELECT 'notebooks' AS produto, dgn.title FROM d_gold_notebooks dgn
    UNION ALL
    SELECT 'smartphones' AS produto, dgs.title FROM d_gold_smartphone dgs
    UNION ALL
    SELECT 'tv' AS produto, dgtv.title FROM d_gold_tv dgtv
    UNION ALL
    SELECT 'tablets' AS produto, dgt.title FROM d_gold_tablets dgt
    UNION ALL
    SELECT 'smartwach' AS produto, dgs2.title FROM d_gold_smartwatch dgs2
) AS all_titles
GROUP BY produto;

SELECT 
    produto,
    COUNT(DISTINCT title) AS num_produtos,
    website,
    brand
FROM (
    SELECT 'notebooks' AS produto, dgn.title, fg.website, dgn.brand FROM d_gold_notebooks dgn
    join f_gold fg on dgn.id = fg.id
    UNION ALL
    SELECT 'smartphones' AS produto, dgs.title, fg.website, dgs.brand FROM d_gold_smartphone dgs
     join f_gold fg on dgs.id = fg.id
    UNION ALL
    SELECT 'tv' AS produto, dgtv.title, fg.website ,dgtv.brand FROM d_gold_tv dgtv
    join f_gold fg on dgtv.id = fg.id
    UNION ALL
    SELECT 'tablets' AS produto, dgt.title, fg.website, dgt.brand FROM d_gold_tablets dgt
       join f_gold fg on dgt.id = fg.id
    UNION ALL
    SELECT 'smartwatchs' AS produto, dgs2.title, fg.website, dgs2.brand FROM d_gold_smartwatch dgs2 
       join f_gold fg on dgs2.id = fg.id
) AS all_titles
GROUP BY produto, website, brand
order by website;

WITH ranked_products AS (
    SELECT 
        produto,
        title AS num_produtos,
        website,
        brand,
        discount_price,
        created_at,
        updated_at,
        link,
        ROW_NUMBER() OVER (PARTITION BY produto, website, brand ORDER BY updated_at ASC) AS rn 
    FROM (
        SELECT 'notebooks' AS produto, dgn.title, dgn.discount_price, fg.website, dgn.brand, fg.created_at, fg.updated_at,dgn.link
        FROM d_gold_notebooks dgn
        JOIN f_gold fg ON dgn.id = fg.id
        UNION ALL
        SELECT 'smartphones' AS produto, dgs.title, dgs.discount_price, fg.website, dgs.brand, fg.created_at, fg.updated_at,dgs.link
        FROM d_gold_smartphone dgs
        JOIN f_gold fg ON dgs.id = fg.id
        UNION ALL
        SELECT 'tv' AS produto, dgtv.title, dgtv.discount_price, fg.website, dgtv.brand, fg.created_at, fg.updated_at,dgtv.link
        FROM d_gold_tv dgtv
        JOIN f_gold fg ON dgtv.id = fg.id
        UNION ALL
        SELECT 'tablets' AS produto, dgt.title, dgt.discount_price, fg.website, dgt.brand, fg.created_at, fg.updated_at,dgt.link
        FROM d_gold_tablets dgt
        JOIN f_gold fg ON dgt.id = fg.id
        UNION ALL
        SELECT 'smartwatchs' AS produto, dgs2.title, dgs2.discount_price, fg.website, dgs2.brand, fg.created_at, fg.updated_at,dgs2.link
        FROM d_gold_smartwatch dgs2
        JOIN f_gold fg ON dgs2.id = fg.id
    ) AS all_titles
)
SELECT 
    produto,
    num_produtos,
    website,
    brand,
    created_at,
    updated_at,
    discount_price,
    link
FROM ranked_products
ORDER BY produto, website, brand, created_at, updated_at ASC;


WITH latest_notebooks AS (
    SELECT 
        dgn.title,
        dgn.discount_price,
        dgn.original_price,
        dgn.brand,
        dgn.rating,
        dgn.link,
        dgn.free_freight,
        dgn.model,
        dgn.cpu,
        dgn.gpu,
        dgn.ram,
        dgn.ssd,
        dgn.specifics,
        dgn.cpu_category,
        fg.created_at,
        fg.updated_at,
        fg.website,
        ROW_NUMBER() OVER (PARTITION BY dgn.title, fg.created_at ORDER BY fg.updated_at DESC) AS rn
    FROM d_gold_notebooks dgn
    JOIN f_gold fg ON dgn.id = fg.id
)
SELECT 
    title, 
    discount_price, 
    original_price, 
    brand, 
    rating, 
    link, 
    free_freight, 
    model, 
    cpu, 
    gpu, 
    ram, 
    ssd, 
    specifics, 
    cpu_category, 
    created_at, 
    updated_at, 
    website
FROM latest_notebooks
WHERE rn = 1 
ORDER BY created_at, title;


WITH latest_notebooks AS (
    SELECT 
        dgn.title,
        dgn.discount_price,
        dgn.original_price,
        dgn.brand,
        dgn.rating,
        dgn.link,
        dgn.free_freight,
        dgn.model,
        dgn.ram,
        dgn.specifics,
        fg.created_at,
        fg.updated_at,
        fg.website,
        ROW_NUMBER() OVER (PARTITION BY dgn.title, fg.created_at ORDER BY fg.updated_at DESC) AS rn
    FROM d_gold_smartphone dgn
    JOIN f_gold fg ON dgn.id = fg.id
)
SELECT 
    title, 
    discount_price, 
    original_price, 
    brand, 
    rating, 
    link, 
    free_freight, 
    model, 
    specifics, 
    ram,
    created_at, 
    updated_at, 
    website
FROM latest_notebooks
WHERE rn = 1 
ORDER BY created_at, title;