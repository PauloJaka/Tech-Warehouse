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