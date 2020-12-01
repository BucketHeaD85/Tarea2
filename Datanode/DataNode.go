package main

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"

	"../proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type asignacion struct {
	posDireccion int64
	numChunk     int64
}

type server struct{}

//Variables para gestionar la comunicacion
var contadorChunks = int64(-1)
var totalChunks = int64(0)
var titulo string
var eleccion int
var chunkSize = 250000
var indice = 0
var contadorMensajes = int64(0)

var mutexChunks = &sync.Mutex{}
var condChunk = sync.NewCond(mutexChunks)

type clientGRPC struct {
	conn      *grpc.ClientConn
	client    proto.ServicioSubidaClient
	chunkSize int
}

//Conexiones entre data nodes
var clienteA clientGRPC

//var clienteB clientGRPC
//var clienteC clientGRPC

/*
var mutexNodoA = &sync.Mutex{}
var condNodoA = sync.NewCond(mutexNodoA)

var mutexNodoB = &sync.Mutex{}
var condNodoB= sync.NewCond(mutexNodoB)

var mutexNodoC = &sync.Mutex{}
var condNodoC = sync.NewCond(mutexNodoC)
*/

//Direcciones ip relevantes
var direcciones = [3]string{"10.6.40.246:50053", "10.6.40.247:50053", "10.6.40.248:50053"}

//var direcciones = [3]string{"localhost:50052", "localhost:50053", "localhost:50054"}

//var disponibles = [3]bool{true,true,true}
//var nameNode = "localhost:50055"

var nameNode = "10.6.40.249:50055"

func gestionEnvios(cliente clientGRPC, nodo int, nombre string) {
	cliente = preparar(nodo)
	cliente.distribuirChunks(context.Background(), nombre)
	cliente.conn.Close()
}

func newClient(direccion string) (c clientGRPC, err error) {

	c.chunkSize = 250000

	c.conn, err = grpc.Dial(direccion, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("failed to start grpc connection with address" + direccion)
		return
	}

	c.client = proto.NewServicioSubidaClient(c.conn)

	return c, err
}

func conectarDataNode(direccion int) clientGRPC {
	var tecla string
	fmt.Println("Direccion de nodo destino" + direcciones[direccion])
	c1, err := newClient(direcciones[direccion])
	for err != nil {
		//	disponibles[direccion] = false
		fmt.Println("Error al conectar al Data Node de direccion " + direcciones[direccion] + ". Presione 'r' para reintentar, o cualquier otra para omitir.")
		fmt.Scanf("%s", &tecla)
		if strings.ToLower(tecla) == "r" {
			c1, err = newClient(direcciones[direccion])
			/*
				if(err == nil){
					disponibles[direccion] = true
				}*/
		} else {
			break
		}

	}
	return c1
}

func preparar(nodo int) clientGRPC {
	cliente := conectarDataNode(nodo)
	return cliente
}

func main() {
	/*
		fmt.Println("Puerto")

		_, err := fmt.Scanf("%d", &eleccion)
		if err != nil {
			fmt.Println(err)
		}
	*/
	eleccion = 1
	var puerto = ":50053"
	/*
		var puerto string
		if eleccion == 0 {
			puerto = ":50052"
		} else {
			if eleccion == 1 {
				puerto = ":50053"
			} else {
				puerto = ":50054"
			}
		}
	*/
	listener, err := net.Listen("tcp", puerto)
	if err != nil {
		log.Fatalf("failed to listen on port: %v", err)
	}

	srv := grpc.NewServer()
	proto.RegisterServicioSubidaServer(srv, &server{})
	reflection.Register(srv)
	fmt.Println("Escuchando")
	if e := srv.Serve(listener); e != nil {
		log.Fatalf("failed to Serve on port 4040: %v", e)
	}

}

func (c *clientGRPC) distribuirChunks(ctx context.Context, f string) (err error) {
	var (
		writing = true
		buf     []byte
		n       int
		file    *os.File
		status  *proto.StatusSubida
	)

	file, err = os.Open(f)
	if err != nil {
		log.Fatalf("Error al abrir el archivo: %v", err)
		return
	}
	defer file.Close()

	stream, err := c.client.SubirArchivo(ctx)
	if err != nil {
		log.Fatalf("Failed to create upstream for file:%v", err)
		return
	}
	defer stream.CloseSend()

	req := &proto.Chunk{
		Data: &proto.Chunk_Nombre{
			Nombre: f,
		},
	}

	err = stream.Send(req)

	if err != nil {
		log.Fatalf("Cannot send file info")
	}

	//stats.StartedAt = time.Now()
	buf = make([]byte, c.chunkSize)
	for writing {
		n, err = file.Read(buf)
		if err != nil {
			if err == io.EOF {
				writing = false
				err = nil
				continue
			}

			log.Fatalf("Error while copying from file to buf:%v", err)
			return
		}

		req := &proto.Chunk{
			Data: &proto.Chunk_Contenido{
				Contenido: buf[:n],
			},
		}

		err = stream.Send(req)
		fmt.Println("Chunk enviado a data node")
	}

	//stats.FinishedAt = time.Now()

	status, err = stream.CloseAndRecv()
	if err != nil {
		log.Fatalf("failed to receive upstream status response:%v", err)
		return
	}

	if status.Codigo != proto.CodigoStatusSubida_Exitoso {
		log.Fatalf("upload failed - msg:%v", err)
		return
	}

	return
}

func generarPropuesta(numChunks int64) error {
	asignaciones := make([]*proto.Asignacion, numChunks)
	//fmt.Println("NumeroChunks:", numChunks)
	pos := int64(-1)
	for i := int64(0); i < numChunks; i++ {
		//dado := rand.Float64()
		asignaciones[i] = new(proto.Asignacion)
		asignaciones[i].PosDireccion = (pos + 1) % 3
		pos++
		asignaciones[i].NumChunk = i + int64(1)
		fmt.Println("Generated proposal:" + direcciones[asignaciones[i].PosDireccion])
	}

	conn, err := grpc.Dial(nameNode, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to connect: %s", err)
	}
	client := proto.NewServicioNameNodeClient(conn)
	propuesta := proto.Propuesta{
		Asignacion: asignaciones,
		Titulo:     titulo,
		Nchunks:    numChunks,
		IdNodo:     int64(eleccion),
	}

	solicitud := proto.Request{
		Id: int64(eleccion),
	}
	//fmt.Println(len(propuesta.Asignacion))
	//fmt.Println("Largo de asignaciones:%v", len(asignaciones))
	//fmt.Println("Numero de chunks:", numChunks)
	client.SolicitarAcceso(context.Background(), &solicitud)
	respuesta, _ := client.Confirmar(context.Background(), &propuesta)
	contadorMensajes = respuesta.GetIdNodo()
	for i := int64(0); i < numChunks; i++ {
		fmt.Println("Titulo:" + titulo)
		nodo := int(respuesta.GetAsignacion()[i].PosDireccion)
		nombre := titulo + "_" + strconv.FormatInt(i, 10)
		fmt.Println("Nombre:" + nombre)
		if nodo == eleccion {
			//ioutil.WriteFile(nombre, recepcion.Contenido, os.ModeAppend)
			continue
		}
		contadorMensajes++
		//go gestionEnvios(clienteA, nodo, nombre)
		gestionEnvios(clienteA, nodo, nombre)
	}
	fmt.Printf("%d mensajes enviados para la distribucion de %v\n", contadorMensajes, titulo)

	//fmt.Println(respuesta.GetAsignacion())
	return nil

}

func (server *server) AcusoEnvio(ctx context.Context, nChunks *proto.Setup) (*proto.Setup, error) {
	condChunk.L.Lock()
	contadorChunks = nChunks.GetNumChunks()
	titulo = nChunks.GetTitulo()
	totalChunks = contadorChunks
	//indice = 0
	return &proto.Setup{NumChunks: contadorChunks, Titulo: titulo}, nil
}

func (server *server) SubirArchivo(stream proto.ServicioSubida_SubirArchivoServer) (err error) {
	req, err := stream.Recv()
	if err != nil {
		log.Fatalf("Error al recibir informacion sobre el archivo: %v", err)
		return nil
	}
	nombre := req.GetNombre()
	for {
		recepcion, e := stream.Recv()
		if e != nil {
			if e == io.EOF {
				goto END
			}

			log.Fatalf("failed unexpectadely while reading chunks from stream")
			return
		}
		newFileName := nombre
		fmt.Println("Filename:" + newFileName)
		indice++
		//contadorChunks++
		_, err = os.Create(newFileName)

		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		ioutil.WriteFile(newFileName, recepcion.GetContenido(), os.ModeAppend)

	}

END:
	// once the transmission finished, send the
	// confirmation if nothign went wrong

	err = stream.SendAndClose(&proto.StatusSubida{
		Mensaje: "Archivo recibido",
		Codigo:  proto.CodigoStatusSubida_Exitoso,
	})
	contadorChunks--
	if contadorChunks == 0 {
		indice = 0
		//fmt.Println("Going to the other side")
		generarPropuesta(totalChunks)
		condChunk.L.Unlock()
	}
	return

}

func (server *server) DescargarArchivo(request *proto.Solicitud, stream proto.ServicioSubida_DescargarArchivoServer) error {
	archivo := request.GetNombreChunk()
	final := archivo[len(archivo)-1:]
	numero, _ := strconv.Atoi(final)
	//numero--
	fmt.Println(strconv.Itoa(numero))
	letras := strconv.Itoa(numero)
	archivoReal := archivo[:len(archivo)-2] + ".pdf_" + letras
	file, err := os.Open(archivoReal)
	if err != nil {
		log.Fatalf("Error al abrir el archivo: %v", err)
		return err
	}

	defer file.Close()
	buf := make([]byte, chunkSize)
	n, err := file.Read(buf)
	if err != nil {
		log.Fatalf("Error while copying from file to buf:%v", err)
		return err
	}

	req := &proto.Chunk{
		Data: &proto.Chunk_Contenido{
			Contenido: buf[:n],
		},
	}

	err = stream.Send(req)

	if err != nil {
		log.Fatalf("Failed to send chunk over stream:%v", err)
		return err
	}
	fmt.Println("Chunk enviado a cliente")

	return nil

}
