delete_query = [
    "MATCH (b:Book) DETACH DELETE b;",
    "MATCH (c:Customer) DETACH DELETE c;",
    "MATCH (o:Order) DETACH DELETE o;"
]

create_query = """
CREATE 
(b1:Book {book_id: 1, genre: "Fantasy", title: "Poppy War", author: "R F Kuang", price: 20}),
(b2:Book {book_id: 2, genre: "Science Fiction", title: "Dune", author: "Frank Herbert", price: 22}),
(b3:Book {book_id: 3, genre: "Fiction", title: "Punk 57", author: "Penelope Douglas", price: 15}),
(b4:Book {book_id: 4, genre: "Mystery", title: "The Girl with the Dragon Tattoo", author: "Stieg Larsson", price: 20}),
(b5:Book {book_id: 5, genre: "Romance", title: "Twisted Love", author: "Ana Huang", price: 15}),
(b6:Book {book_id: 6, genre: "Non-fiction", title: "Sapiens: A Brief History of Humankind", author: "Yuval Noah Harari", price: 30}),
(b7:Book {book_id: 7, genre: "Horror", title: "Outsider", author: "Stephen King", price: 18}),
(b8:Book {book_id: 8, genre: "Historical", title: "All the Light We Cannot See", author: "Anthony Doerr", price: 28}),
(b9:Book {book_id: 9, genre: "Thriller", title: "Gone Girl", author: "Gillian Flynn", price: 24}),

(c1:Customer {customer_id: 1, name: "Lucy", surname: "Grey", phones: ["016378266", "999826316"], address: "123 One St, Onecity, UK"}),
(c2:Customer {customer_id: 2, name: "Coriolanus", surname: "Snow", phones: ["826357910", "338211900"], address: "456 Two St, Twocity, UK"}),
(c3:Customer {customer_id: 3, name: "Sejanus", surname: "Plint", phones: ["834477511", "087683917"], address: "789 Three St, Threecity, UK"}),
(c4:Customer {customer_id: 4, name: "Katniss", surname: "Everdeen", phones: ["971638199", "000875372"], address: "101 Four St, Fourcity, UK"}),
(c5:Customer {customer_id: 5, name: "Peeta", surname: "Mellark", phones: ["876897184", "123452631"], address: "135 Five St, Fivecity, UK"}),
(c6:Customer {customer_id: 6, name: "Finnick", surname: "Odair", phones: ["987126341"], address: "246 Six St, Sixcity, UK"}),

(o1:Order {order_id: 202401, date: "2024-05-01"}),
(o2:Order {order_id: 202402, date: "2024-05-02"}),
(o3:Order {order_id: 202403, date: "2024-05-03"}),
(o4:Order {order_id: 202404, date: "2024-05-04"}),
(o5:Order {order_id: 202405, date: "2024-05-05"})
"""


bought_query = """
MATCH (c:Customer {customer_id: $customer_id}), (o:Order {order_id: $order_id})
MERGE (c)-[:BOUGHT]->(o)
"""


contains_query = """
MATCH (o:Order {order_id: $order_id}), (b:Book {book_id: $book_id})
MERGE (o)-[:CONTAINS]->(b)
"""


view_query = """
MATCH (c:Customer {customer_id: $customer_id}), (b:Book {book_id: $book_id})
MERGE (c)-[:VIEW]->(b)
"""


update_total_sum_query = """
MATCH (o:Order {order_id: $order_id})
WITH o, [(o)-[:CONTAINS]->(b:Book) | b.price] AS prices
WITH o, reduce(total = 0, price IN prices | total + price) AS new_total_sum
SET o.total_sum = new_total_sum
RETURN o.total_sum
"""
