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
	/*
		if eleccion == 0 {
			clienteB = conectarDataNode(1)
			clienteC = conectarDataNode(2)

		} else {
			if eleccion == 1 {
				clienteA = conectarDataNode(0)
				clienteC = conectarDataNode(2)
			} else {
				clienteA = conectarDataNode(0)
				clienteB = conectarDataNode(1)
			}
		}
	*/
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

/*
func conectarNameNode() {

	conn, err := grpc.Dial(nameNode, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to connect: %s", err)
	}
	client := proto.NewServicioNameNodeClient(conn)
}
*/

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

		err = stream.Send(&proto.Chunk{
			Contenido: buf[:n],
		})
		if err != nil {
			log.Fatalf("Failed to send chunk over stream:%v", err)
			return
		}
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

func generarPropuesta(numChunks int64) {
	asignaciones := make([]*proto.Asignacion, numChunks)
	//fmt.Println("NumeroChunks:", numChunks)
	pos := int64(-1)
	for i := int64(0); i < numChunks; i++ {
		//dado := rand.Float64()
		asignaciones[i] = new(proto.Asignacion)
		/*
			if dado <= 0.33 { //asignar a nodo 1
				asignaciones[i].PosDireccion = 0
			} else {
				if dado <= 0.66 { //asignar a nodo 2
					asignaciones[i].PosDireccion = 1

				} else { //asignar a nodo 3
					asignaciones[i].PosDireccion = 2
				}
			}
		*/
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
	}

	solicitud := proto.Request{
		Id: int64(eleccion),
	}
	//fmt.Println(len(propuesta.Asignacion))
	//fmt.Println("Largo de asignaciones:%v", len(asignaciones))
	//fmt.Println("Numero de chunks:", numChunks)
	client.SolicitarAcceso(context.Background(), &solicitud)
	respuesta, _ := client.Confirmar(context.Background(), &propuesta)

	for i := int64(0); i < numChunks; i++ {
		fmt.Println("Titulo:" + titulo)
		nodo := int(respuesta.GetAsignacion()[i].PosDireccion)
		nombre := titulo + "_" + strconv.FormatInt(i, 10)
		fmt.Println("Nombre:" + nombre)
		if nodo == eleccion {
			//ioutil.WriteFile(nombre, recepcion.Contenido, os.ModeAppend)
			continue
		}
		go gestionEnvios(clienteA, nodo, nombre)
	}

	fmt.Println(respuesta.GetAsignacion())

}

func (server *server) AcusoEnvio(ctx context.Context, nChunks *proto.Setup) (*proto.Setup, error) {
	condChunk.L.Lock()
	contadorChunks = nChunks.GetNumChunks()
	titulo = nChunks.GetTitulo()
	totalChunks = contadorChunks
	indice = 0
	return &proto.Setup{NumChunks: contadorChunks, Titulo: titulo}, nil
}

func (server *server) SubirArchivo(stream proto.ServicioSubida_SubirArchivoServer) (err error) {
	for {
		recepcion, e := stream.Recv()
		if e != nil {
			if e == io.EOF {
				goto END
			}

			log.Fatalf("failed unexpectadely while reading chunks from stream")
			return
		}
		newFileName := titulo + "_" + strconv.Itoa(indice)
		indice++
		//contadorChunks++
		_, err = os.Create(newFileName)

		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		ioutil.WriteFile(newFileName, recepcion.Contenido, os.ModeAppend)

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
		//fmt.Println("Going to the other side")
		generarPropuesta(totalChunks)
		condChunk.L.Unlock()
	}
	//*************************************************************************
	/*
		newFileName := "NEWbigfile.pdf"
		_, err = os.Create(newFileName)

		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		//set the newFileName file to APPEND MODE!!
		// open files r and w

		file, err := os.OpenFile(newFileName, os.O_APPEND|os.O_WRONLY, os.ModeAppend)

		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		// IMPORTANT! do not defer a file.Close when opening a file for APPEND mode!
		// defer file.Close()

		// just information on which part of the new file we are appending
		//var writePosition int64 = 0

		for i := uint64(1); i < contadorChunks; i++ {

			//read a chunk
			currentChunkFileName := "bigfile_" + strconv.FormatUint(i, 10)

			newFileChunk, err := os.Open(currentChunkFileName)

			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}

			defer newFileChunk.Close()

			chunkInfo, err := newFileChunk.Stat()

			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}

			// calculate the bytes size of each chunk
			// we are not going to rely on previous data and constant

			var chunkSize int64 = chunkInfo.Size()
			chunkBufferBytes := make([]byte, chunkSize)

			fmt.Println("Appending at position : [", writePosition, "] bytes")
			writePosition = writePosition + chunkSize

			// read into chunkBufferBytes
			reader := bufio.NewReader(newFileChunk)
			_, err = reader.Read(chunkBufferBytes)

			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}

			// DON't USE ioutil.WriteFile -- it will overwrite the previous bytes!
			// write/save buffer to disk
			//ioutil.WriteFile(newFileName, chunkBufferBytes, os.ModeAppend)

			n, err := file.Write(chunkBufferBytes)

			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}

			file.Sync() //flush to disk

			// free up the buffer for next cycle
			// should not be a problem if the chunk size is small, but
			// can be resource hogging if the chunk size is huge.
			// also a good practice to clean up your own plate after eating

			chunkBufferBytes = nil // reset or empty our buffer

			fmt.Println("Written ", n, " bytes")

			fmt.Println("Recombining part [", contadorChunks, "] into : ", newFileName)
		}

		// now, we close the newFileName
		file.Close()
	*/
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

	err = stream.Send(&proto.Chunk{
		Contenido: buf[:n],
	})

	if err != nil {
		log.Fatalf("Failed to send chunk over stream:%v", err)
		return err
	}
	fmt.Println("Chunk enviado a cliente")

	return nil

}
