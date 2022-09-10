
import csv
import json
import requests
import sqlite3

import sqlalchemy
from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship

# Crear el motor (engine) de la base de datos llamada "sqlite:///   .db"
engine = sqlalchemy.create_engine("sqlite:///base_dato_productos_mla.db")
base = declarative_base()


class Productos(base):
    __tablename__ = "tablaproductos"
    id = Column(Integer, primary_key=True)
    item_id = Column(String)
    site_id = Column(String)
    title = Column(String)
    price = Column(Integer)
    currency_id = Column(String)
    initial_quantity = Column(Integer)
    available_quantity = Column(Integer)
    sold_quantity = Column(Integer)
   
    def __repr__(self):
        return f"item_id {self.item_id}, site_id {self.site_id}, title {self.title}, price {self.price}, currency_id {self.currency_id}, initial_quantity {self.initial_quantity}, available_quantity {self.available_quantity}, sold_quantity {self.sold_quantity}"




def create_schema():
    # Borrar todas las tablas existentes en la BD
    base.metadata.drop_all(engine)
    # Crear nuevamente todas las tablas
    base.metadata.create_all(engine)


def insert_producto(item_id, site_id, title, price, currency_id, initial_quantity, available_quantity, sold_quantity):
    # Crear la session
    Session = sessionmaker(bind=engine)
    session = Session()

    # Crear un nuevo producto
    nuevo_producto = Productos(item_id=item_id, site_id=site_id, title=title, price=price, currency_id=currency_id, initial_quantity=initial_quantity, available_quantity=available_quantity, sold_quantity=sold_quantity)
   
    # Agregar el nuevo producto a la DB
    session.add(nuevo_producto)
    session.commit()
    

def fill():
    # Leer el archivo .CSV que contiene las columnas site e id de los productos
    archivo= "meli_technical_challenge_data.csv"
    with open(archivo) as csvfile:
        data = list(csv.DictReader(csvfile))

    # item_id= site + id
    item_id = [(row["site"] + row["id"]) for row in data]
       
    url = "https://api.mercadolibre.com/items?ids="
 
    for x in range(len(item_id)): 
        response = requests.get(url + item_id[x])
        data = response.json()

        # filtro los datos vacios "None" y guardo los demas en la tabla "tablaproductos" dentor de la DB "base_dato_productos_mla.db"
        if data[0]["body"].get("id") != None and data[0]["body"].get("site_id") != None and data[0]["body"].get("title") != None and data[0]["body"].get("price") != None and data[0]["body"].get("currency_id") != None and data[0]["body"].get("initial_quantity") != None and data[0]["body"].get("available_quantity") != None and data[0]["body"].get("sold_quantity") != None:
            insert_producto(data[0]["body"].get("id"), data[0]["body"].get("site_id"), data[0]["body"].get("title"), data[0]["body"].get("price"), data[0]["body"].get("currency_id"), data[0]["body"].get("initial_quantity"), data[0]["body"].get("available_quantity"), data[0]["body"].get("sold_quantity"))
    return 


def fetch(item_id): 
    # Crear la session
    Session = sessionmaker(bind=engine)
    session = Session()

    query = session.query(Productos).filter(Productos.item_id == item_id)
    search = query.first()

    if search != None :
        print(search)
    else:
        print("Producto no encontrado")
    return


if __name__ == '__main__':
    
    # reset and create database (DB)
    create_schema()   

    fill()
    
    # Leer filas segun item_id
    fetch("MLA845041373")
    fetch("MLA717159516")
