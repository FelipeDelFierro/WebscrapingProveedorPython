
import requests
from bs4 import BeautifulSoup
import pyodbc
import pandas as pd
import re
#connection class
class Connection:
    con = pyodbc.connect(DRIVER='{SQL Server}',SERVER='server',DATABASE='database',UID='sa',PWD='password')
    cursor = con.cursor()

#new command  (cmd) 
cursor = Connection.cursor
#crear dataset productos
def getProducto(codigoProducto):
    cursor.execute("""SELECT Nombre, codigoProducto, Precio FROM comProducto as p 
    inner join comProveedorProducto as pp on p.IdProducto = pp.IdProducto where p.codigoProducto = '{}'""".format(codigoProducto))
    producto = cursor.fetchone()
    return producto


#headers 
headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.119 Safari/537.36'}
#credenciales login si es necesario 
login_data={
    'email': '',
    'passwd': '',
    'back':'' ,
    'SubmitLogin': ''
    }

contador= 0
#creo una sesion con login en el sitio web 
with requests.Session() as s:
     #ingreso la pagina donde esta el login  
      urlBase ="https://www.lasecretaria.cl/"
      urlAutenticacion = "https://www.lasecretaria.cl/autenticacion"
      r = s.get(urlAutenticacion,headers=headers)
      
      #scraping para el login
      soupLogin = BeautifulSoup(r.content,'html5lib')
      login_data['login_form'] = soupLogin.find('input', attrs={'name':'email','name':'passwd'})['value']
      print("Autenticando en el sitio web...")
      r = s.post(urlAutenticacion,data=login_data,headers=headers)
      #end scraping login

      #obtener links de categorias
      def getLinkCategorias(url,headers,session):
        """function that gets all link of a sidebar of a website and return a list of links
            the tags must be changes according the website that you want to scrape"""
        try:
            LinkCategorias = []    
            session = s.post(url,headers=headers)
            soupLinkCategorias = BeautifulSoup(session.content,'html5lib')
            links= soupLinkCategorias.find_all('ul' , attrs={'nav navbar-nav megamenu right'})

            for link in links:
                for li in link.find_all('li'):
                    a =li.find('a')
                    LinkCategorias.append(a['href'])   
        except Exception as ex:
            print("cannot get categories, please review the web tag, url or the login data. Error: " + str(ex))
        return LinkCategorias

      #almaceno links de categorias
      Links = getLinkCategorias(urlBase,headers,r)
      
      #scraping productos
      for x in Links:
        link = x + '?id_category=&n=100000'
        r = s.post(link, headers=headers)
        soupProductos = BeautifulSoup(r.content,'html5lib')

        #verifico donde esta el div de productos
        productos = soupProductos.find_all('div', attrs={'class':'product-meta'})
        # recorro cada producto
        cantidadProductos= len(productos)
        try:
            CategoriaGenerica = soupProductos.find('span', attrs={'class':'navigation_page'}).text
            CategoriaGenericaTag = soupProductos.find('span', attrs={'class':'navigation_page'})
            if CategoriaGenerica is None:
                pass
            else:
                print("Registrando " + str(cantidadProductos) + " para la categoria " + CategoriaGenerica)
        except AttributeError as e:
            print("Registrando "+ str(cantidadProductos) + "para categoria desconocida con tag" + str(CategoriaGenericaTag) )  
        for producto in productos:
            
            #asigno valor a variables 
            
            SubCategoria= "NULL"
            IdUnidad = '1'
            IdCuenta = '1'
            IdProveedor = '6'
            nombreProducto = producto.find('h5', attrs={'class': 'name'}).text
            linkProducto = producto.find('a', attrs={'class':'product-name'})
            linkFinal= linkProducto['href']
            precio = producto.find('span', attrs={'class': 'price'}).text
            str(precio).replace('.','')
            precio2 = precio.replace('.','')
           
            #recorrer link productos detalle
            rInterno = s.post(linkFinal, headers=headers)
            soupAtributos = BeautifulSoup(rInterno.content,'html5lib')
            imagenBruto = soupAtributos.find(itemprop="image")
            ImagenLink= imagenBruto["src"]
            codigoProducto = soupAtributos.find(itemprop="sku").text
            productoDB = getProducto(codigoProducto)
            # if productoDB[2] ==str(precio2).replace('$',''):
            #     pass
            # else:
            #     contador +=1
            # if str(productosDataset).find(codigoProducto) >0:
            #         pass
                
            # else:
            #obtener subcategoria
            def getCategoriaSubCategoria(soupAtributos):
                subcategorias = []
                SubCategoriaAtributo = soupAtributos.find_all('div', attrs={'class':'breadcrumb clearfix'})
                for data in SubCategoriaAtributo:
                    for a in data.find_all('a'):
                        subcategorias.append(a.text)
                        #valido que contenga subcategoria
                        if len(subcategorias)== 2:
                            Categoria = subcategorias[1]
                            subcatFinal= "NULL"
                        elif len(subcategorias)== 3:
                            Categoria = subcategorias[1]
                            subcatFinal= subcategorias[2]

                return Categoria,subcatFinal
            Categoria = getCategoriaSubCategoria(soupAtributos)[0]
            subcatFinal = getCategoriaSubCategoria(soupAtributos)[1]
                     
            sql = """exec usp_webscrapingInsertProveedor 
                             @nombreProducto = ?,@IdUnidad=? ,
                            @idCuenta=?, @categoria=?, @subcategoria=?, @ImagenLink=?, @linkProducto=?, @codigoProducto=?, @IdProveedor=?,
                            @precio=?
                         """
            values = (nombreProducto,IdUnidad,IdCuenta,Categoria,subcatFinal,ImagenLink,linkFinal,codigoProducto,IdProveedor,precio2)
            cursor.execute(sql,values)
            
            cursor.commit()
            contador +=1   
        print("Guardando registros para la categoria "+ str(Categoria) + "en la base de datos")
print("Se Verificaron "+  str(contador) + " registros en total para el proveedor")





# def Autenticar(url,login_data,agent):
#     s = requests.Session()
#     try:
#         r = s.get(url,headers=agent)
#         status= r.status_code
        
#         if status != 200:
#             print("Error de autenticacion")
#         else:
#             print("Ya estas Logeado")
#             r = s.post(url, headers=headers)
            
#     except requests.HTTPError as e:
#         print("Http Error")
#     except ConnectionError as e:
#         print("error de conexion con el servidor")
#     return r





#  """
#             begin transaction
#             DECLARE @id int 
#             IF EXISTS (SELECT codigoProducto , IdProducto FROM comProducto where codigoProducto ='{}') 
#              begin
             
#              UPDATE comProducto
#              set  Nombre = '{}', FechaModificacion = getdate()
#              where codigoProducto = '{}'
             
#              UPDATE comProveedorProducto
#              SET Precio = '{}'
#              WHERE 

#              end
#              else
#              begin
#              INSERT INTO comProducto(Nombre,IdTipo,IdUnidad,idCuenta,Activo,FechaCreacion,IdUsuarioCreacion,Categoria,Imagen,link,codigoProducto) VALUES('{}','1','1','4','1',getdate(),'0','{}','{}','{}','{}')
#              set @id  = SCOPE_IDENTITY()   
#              INSERT INTO comProveedorProducto(IdProveedor,IdProducto,Precio,FechaIngreso,IdUsuarioIngreso) values ('6',@id,'{}',getdate(),'0')
#              end
#              commit transaction
#              """.format(codigoProducto,nombreProducto,codigoProducto,precio2,nombreProducto,Categoria,ImagenLink,linkFinal,codigoProducto , precio2)
        


