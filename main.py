from neo4j import GraphDatabase, basic_auth
from queries import delete_query, create_query, bought_query, contains_query, view_query, update_total_sum_query
import pprint
from pyvis.network import Network

print('1. task one. create a model of online store (i chose bookstore for this example).')

driver = GraphDatabase.driver(
  "bolt://3.95.194.185:7687",
  auth=basic_auth("neo4j", "semaphores-boys-west"))

cypher_query = '''
MATCH (n)
RETURN COUNT(n) AS count
LIMIT $limit
'''

with driver.session(database="neo4j") as session:
  results = session.read_transaction(
    lambda tx: tx.run(cypher_query,
                      limit=10).data())
  for record in results:
    print(record['count'])



# Initializes the graph database
def create_data():
    with driver.session() as session:
        for query in delete_query:
            session.run(query)
        session.run(create_query)  

# Adds CONTAINS relationships between orders and items.
def add_contains_relationship(order_id, book_id):
    with driver.session() as session:
        session.run(contains_query, order_id=order_id, book_id=book_id)
        session.run(update_total_sum_query, order_id=order_id)

#  Adds BOUGHT relationships between customers and orders
def add_bought_relationship(customer_id, order_id):
    with driver.session() as session:
        session.run(bought_query, customer_id=customer_id, order_id=order_id)

# Adds VIEW relationships between customers and items
def add_view_relationship(customer_id, book_id):
    with driver.session() as session:
        session.run(view_query, customer_id=customer_id, book_id=book_id)


create_data()

add_contains_relationship(202401, 1)
add_contains_relationship(202402, 2)
add_contains_relationship(202402, 3)
add_contains_relationship(202403, 5)
add_contains_relationship(202404, 7)
add_contains_relationship(202405, 2)
add_contains_relationship(202403, 1)
add_contains_relationship(202401, 2)
add_contains_relationship(202404, 4)

add_bought_relationship(1, 202401)
add_bought_relationship(2, 202402)
add_bought_relationship(2, 202401)
add_bought_relationship(3, 202404)
add_bought_relationship(4, 202402)
add_bought_relationship(4, 202403)
add_bought_relationship(5, 202405)

add_view_relationship(1, 2)
add_view_relationship(1, 7)
add_view_relationship(3, 5)
add_view_relationship(3, 1)
add_view_relationship(3, 2)
add_view_relationship(6, 9)


print('2. find items in some exact order (by order_id)')

def find_books_in_order(order_id):
    with driver.session() as session:
        result = session.run(
            """
            MATCH (o:Order {order_id: $order_id})-[:CONTAINS]->(b:Book)
            RETURN b
            """,
            order_id=order_id
        )
        books = [record["b"]._properties for record in result]
        return books

order_id = 202403
books_in_order = find_books_in_order(order_id)
pprint.pprint(books_in_order)


print('3. get the total of some exact order')

def get_order_total(order_id):
    with driver.session() as session:
        result = session.run(
            """
            MATCH (o:Order {order_id: $order_id})
            RETURN o.total_sum AS total_cost
            """,
            order_id=order_id
        )
        total_cost = result.single()["total_cost"]
        return total_cost

order_id = 202402
total_cost = get_order_total(order_id)
print(f"Total cost of order {order_id}: ${total_cost}")


print('4. get all orders from some exact customer')

def find_orders_by_customer(customer_id):
    with driver.session() as session:
        result = session.run(
            """
            MATCH (c:Customer {customer_id: $customer_id})-[:BOUGHT]->(o:Order)
            RETURN o
            """,
            customer_id=customer_id
        )
        orders = [record["o"]._properties for record in result]
        return orders

customer_id = 4
orders = find_orders_by_customer(customer_id)
pprint.pprint(orders)


print('5. get all items bought by some exact customer (by their orders)')

def find_books_by_customer(customer_id):
    with driver.session() as session:
        result = session.run(
            """
            MATCH (c:Customer {customer_id: $customer_id})-[:BOUGHT]->(:Order)-[:CONTAINS]->(b:Book)
            RETURN b
            """,
            customer_id=customer_id
        )
        books = [record["b"]._properties for record in result]
        return books

customer_id = 4
books = find_books_by_customer(customer_id)
pprint.pprint(books)


print('6. get a number of items bought by some exact customer (by their order)')

def count_books_bought_by_customer(customer_id):
    with driver.session() as session:
        result = session.run(
            """
            MATCH (c:Customer {customer_id: $customer_id})-[:BOUGHT]->(:Order)-[:CONTAINS]->(b:Book)
            RETURN COUNT(b) AS total_books_bought
            """,
            customer_id=customer_id
        )
        total_books_bought = result.single()["total_books_bought"]
        return total_books_bought

customer_id = 4
total_books_bought = count_books_bought_by_customer(customer_id)
print("Total books bought by customer:", total_books_bought)


print('7. find a total for customer purchase (by their order)')

def total_spent_by_customer(customer_id):
    with driver.session() as session:
        result = session.run(
            """
            MATCH (c:Customer {customer_id: $customer_id})-[:BOUGHT]->(o:Order)
            RETURN SUM(o.total_sum) AS total_spent_by_customer
            """,
            customer_id=customer_id
        )
        total_spent_by_customer = result.single()["total_spent_by_customer"]
        return total_spent_by_customer

customer_id = 4
total_spent = total_spent_by_customer(customer_id)
print("Total spent by customer:", total_spent)


print('8. how many times each book was purchased? (+sort by this value)')

def count_purchases_per_book():
    with driver.session() as session:
        result = session.run(
            """
            MATCH (b:Book)<-[:CONTAINS]-(:Order)
            RETURN b, COUNT(*) AS purchases
            ORDER BY purchases DESC
            """
        )
        purchases_per_book = [(record["b"], record["purchases"]) for record in result]
        return purchases_per_book

purchases_per_book = count_purchases_per_book()
for book, purchases in purchases_per_book:
    print(f"Book {book['book_id']} was purchased {purchases} times")


print('9. get all items viewed by some exact customer')

def find_viewed_books(customer_id):
    with driver.session() as session:
        result = session.run(
            """
            MATCH (c:Customer {customer_id: $customer_id})-[:VIEW]->(b:Book)
            RETURN b
            """,
            customer_id=customer_id
        )
        viewed_books = [record["b"]._properties for record in result]
        return viewed_books

customer_id = 1
viewed_books = find_viewed_books(customer_id)
pprint.pprint(viewed_books)


print('10. get other items that were purchased with some exact item')

def find_related_books(book_id):
    with driver.session() as session:
        result = session.run(
            """
            MATCH (b1:Book {book_id: $book_id})<-[:CONTAINS]-(o:Order)-[:CONTAINS]->(b2:Book)
            WHERE b1 <> b2
            RETURN DISTINCT b2
            """,
            book_id=book_id
        )
        books = [record["b2"]._properties for record in result]
        return books

book_id = 5
related_books = find_related_books(book_id)
pprint.pprint(related_books)


print('11. get customers that purchased some exact item')

def find_customers_who_bought_book(book_id):
    with driver.session() as session:
        result = session.run(
            """
            MATCH (c:Customer)-[:BOUGHT]->(:Order)-[:CONTAINS]->(b:Book)
            WHERE b.book_id = $book_id
            RETURN DISTINCT c
            """,
            book_id=book_id
        )
        customers = [record["c"]._properties for record in result]
        return customers

book_id = 1
customers = find_customers_who_bought_book(book_id)
pprint.pprint(customers)


print('12. get items that some exact customer viewed but didnt buy')

def find_books_viewed_but_not_bought(customer_id):
    with driver.session() as session:
        result = session.run(
            """
            MATCH (c:Customer {customer_id: $customer_id})-[:VIEW]->(b:Book)
            WHERE NOT EXISTS {
                (c)-[:BOUGHT]->(:Order)-[:CONTAINS]->(b)
            }
            RETURN b
            """,
            customer_id=customer_id
        )
        books = [record["b"]._properties for record in result]
        return books


customer_id = 1
books = find_books_viewed_but_not_bought(customer_id)
pprint.pprint(books)
driver.close()