SELECT COUNT(DISTINCT title) AS num_produtos
FROM (
    SELECT dgn.title FROM d_gold_notebooks dgn
    UNION ALL
    SELECT dgs.title FROM d_gold_smartphone dgs
    UNION ALL
    SELECT dgtv.title FROM d_gold_tv dgtv
    UNION ALL
    SELECT dgt.title FROM d_gold_tablets dgt
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
) AS all_titles
GROUP BY produto;
