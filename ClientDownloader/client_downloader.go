package main

import (
	"bufio"
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"time"

	"../proto"
	"google.golang.org/grpc"
)

var direcciones = [3]string{"10.6.40.246:50053", "10.6.40.247:50053", "10.6.40.248:50053"}

//var direcciones = [3]string{"localhost:50052", "localhost:50053", "localhost:50054"}

var namenode = "10.6.40.249:50055"

//var namenode = "localhost:50055"

type datanodeGRPC struct {
	conn      *grpc.ClientConn
	client    proto.ServicioSubidaClient
	chunkSize int
}

type namenodeGRPC struct {
	conn      *grpc.ClientConn
	client    proto.ServicioNameNodeClient
	chunkSize int
}

func conectar(direccion string, tipo int) (c datanodeGRPC, d namenodeGRPC, err error) {

	if tipo == 0 {
		c.conn, err = grpc.Dial(direccion, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(1*time.Second))

		c.client = proto.NewServicioSubidaClient(c.conn)

		c.chunkSize = 250000
		if err != nil {
			fmt.Println(err)
			return
		}

	} else {
		d.conn, err = grpc.Dial(direccion, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(1*time.Second))
		d.client = proto.NewServicioNameNodeClient(d.conn)
		d.chunkSize = 250000
		if err != nil {
			fmt.Println(err)
			return
		}
	}

	return c, d, err
}

func main() {

	_, clienteNameNode, err := conectar(namenode, 1)
	if err != nil {
		log.Fatalf("Error al conectar al namenode")
	}
	request := proto.Request{Id: 1}
	respuesta, err := clienteNameNode.client.VerLibros(context.Background(), &request)
	if err != nil {
		log.Fatalf("El log no esta disponible para ser consultado: %v\n", err)
	}
	largo := len(respuesta.GetLibro())

	fmt.Println("Seleccionar libro:")
	for i := 0; i < largo; i++ {
		fmt.Println(strconv.Itoa(i) + "	:	" + respuesta.GetLibro()[i].Titulo)
	}

	var eleccion int
	_, err = fmt.Scanf("%d", &eleccion)
	if err != nil {
		fmt.Println(err)
	}

	titulo := respuesta.GetLibro()[eleccion].Titulo
	libro := proto.Libro{Titulo: titulo}
	mapa, err := clienteNameNode.client.SolicitarChunks(context.Background(), &libro)
	if err != nil {
		log.Fatalf("Error al conectar al namenode")
	}
	//n := len(mapa.GetAsignacion())
	numChunks := mapa.GetNchunks()
	clienteNameNode.conn.Close()

	newFileName := titulo + ".pdf"
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
	var writePosition int64 = 0

	for i := 0; i < int(numChunks); i++ {
		nodo := int(mapa.GetAsignacion()[i].PosDireccion)
		fmt.Println("Nodo elegido: " + direcciones[nodo])

		nChunk := mapa.GetAsignacion()[i].NumChunk - 1
		nombre := titulo + "_" + strconv.FormatInt(nChunk, 10)
		clienteDataNode, _, _ := conectar(direcciones[nodo], 0)
		pedido := proto.Solicitud{NombreChunk: nombre}
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		stream, err := clienteDataNode.client.DescargarArchivo(ctx, &pedido)
		if err != nil {
			fmt.Println(err)
			log.Fatalf("Rip")
		}

		chunk, err := stream.Recv()
		if err != nil {
			fmt.Println(err)
			log.Fatalf("Rip")
		}

		ioutil.WriteFile(nombre, chunk.GetContenido(), os.ModeAppend)

		currentChunkFileName := titulo + "_" + strconv.FormatInt(nChunk, 10)

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

		n, err := file.Write(chunkBufferBytes)

		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		file.Sync() //flush to disk

		chunkBufferBytes = nil // reset or empty our buffer

		fmt.Println("Written ", n, " bytes")

		fmt.Println("Recombining part [", nChunk, "] into : ", newFileName)

	}

	file.Close()

}
