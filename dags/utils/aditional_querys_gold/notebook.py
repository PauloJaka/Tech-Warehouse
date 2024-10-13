notebook_query = f""" 
(title ILIKE '%Notebook%' OR title ILIKE '%Macbook%' OR title ILIKE '%Laptop%' OR title ILIKE '%Samsugbook%')
  AND title NOT ILIKE '%Cabo%'
  AND title NOT ILIKE '%Mochila%'
  AND title NOT ILIKE '%Capa%'
  AND title NOT ILIKE '%Carregador%'
  AND title NOT ILIKE '%Estojo%'
 and title not ilike '%Suporte%'
 and title not ilike '%Base%'
 and title not ilike '%Fonte%'
 and title not ilike '%Bateria%'
 and title not ilike '%Filtro%'
and title not ilike '%Pasta%'
and title not ilike '%Trava%'
and title not ilike '%The Moe Norman Notebook%'
and title not ilike '%Maleta%'
AND title NOT ILIKE '%Pelicula%'
AND title NOT ILIKE '%Película%'
AND title NOT ILIKE '%Pen drive%'
and link not ilike '%No link%'
AND title NOT ILIKE '%Arshray Kit%'; 
"""