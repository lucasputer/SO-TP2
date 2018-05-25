#include "node.h"
#include "picosha2.h"
#include <stdio.h>
#include <string.h>
#include <pthread.h>
#include <cstdlib>
#include <queue>
#include <atomic>
#include <mpi.h>
#include <map>

int total_nodes, mpi_rank;
Block *last_block_in_chain;
map<string,Block> node_blocks;
pthread_mutex_t broadcast_mutex;

void validate_block(Block* block, bool* hash_validation, bool* node_blocks_validation){
    string hash_hex_str;
    block_to_hash(block,hash_hex_str);

    if(hash_hex_str != block->block_hash){
        *hash_validation = false;
    }

    if(!*node_blocks_validation){
        *node_blocks_validation = node_blocks.count(string(block->block_hash));
    }
}

//Cuando me llega una cadena adelantada, y tengo que pedir los nodos que me faltan
//Si nos separan más de VALIDATION_BLOCKS bloques de distancia entre las cadenas, se descarta por seguridad
bool verificar_y_migrar_cadena(const Block *rBlock, const MPI_Status *status){
    //TODO: Enviar mensaje TAG_CHAIN_HASH

    MPI_Send(rBlock->block_hash, 1, *MPI_BLOCK, status->MPI_SOURCE, TAG_CHAIN_HASH, MPI_COMM_WORLD);

    Block *blockchain = new Block[VALIDATION_BLOCKS];

    //TODO: Recibir mensaje TAG_CHAIN_RESPONSE
    MPI_Status rec_status;
    MPI_Recv(blockchain,1,*MPI_BLOCK,status->MPI_SOURCE,TAG_CHAIN_RESPONSE,MPI_COMM_WORLD,&rec_status);

    //TODO: Verificar que los bloques recibidos
    //sean válidos y se puedan acoplar a la cadena

    //El primer bloque de la lista contiene el hash pedido y el mismo index que el bloque original.
    if(blockchain[0].block_hash != rBlock->block_hash || rBlock->index != blockchain[0].index){
       printf("[%d] Descarto cadena enviada por %d \n", mpi_rank,status->MPI_SOURCE);
       delete []blockchain;
        return false;
    }

    //El hash del bloque recibido es igual al calculado por la función block_to_hash.
    //Cada bloque siguiente de la lista, contiene el hash definido en previous_block_hash del actual elemento.
    //Cada bloque siguiente de la lista, contiene el índice anterior al actual elemento.
    //algun bloque estaba en el diccionario
    int i = 0;
    //creo diff para saber la cantidad de nodos que tengo que recibir
    //si diff es mas que validation blocks no se que onda.
    int diff = blockchain[0].index - last_block_in_chain->index;

    Block current = blockchain[i];
    Block prev = blockchain[i];
    bool validated = true;
    bool any_in_node_blocks = false;

    while(i < diff && validated){
        validate_block(&current, &validated, &any_in_node_blocks);
        if((i > 0) && (current.block_hash != prev.previous_block_hash || current.index + 1 != prev.index))
            validated = false;
        
        i++;
        prev = current;
        current = current = blockchain[i];
    }

    if(validated && any_in_node_blocks){
        //migrar
        i = 0;
        current = blockchain[i];
        while(i < diff){
            node_blocks[string(current.block_hash)] = current;
            i++;
            current = blockchain[i];
        }

        *last_block_in_chain =  blockchain[0];

        delete []blockchain;
        return true;
    }

    delete []blockchain;
    return false;
}

//Verifica que el bloque tenga que ser incluido en la cadena, y lo agrega si corresponde
bool validate_block_for_chain(const Block *rBlock, const MPI_Status *status){
    if(valid_new_block(rBlock)){
        //Agrego el bloque al diccionario, aunque no
        //necesariamente eso lo agrega a la cadena
        node_blocks[string(rBlock->block_hash)]=*rBlock;

        //Si el índice del bloque recibido es 1
        //y mí último bloque actual tiene índice 0,
        //entonces lo agrego como nuevo último.
        if(rBlock->index == 1 && last_block_in_chain->index == 0){
            printf("[%d] Agregado a la lista bloque con index %d enviado por %d \n", mpi_rank, rBlock->index,status->MPI_SOURCE);
            return true;
        }else if(last_block_in_chain->index + 1 == rBlock->index ){
            //chequear si esto esta bien o hay que hacerlo con block_to_hash
            if(rBlock->previous_block_hash == last_block_in_chain->block_hash){
                //Si el índice del bloque recibido es
                //el siguiente a mí último bloque actual,
                //y el bloque anterior apuntado por el recibido es mí último actual,
                //entonces lo agrego como nuevo último.
                *last_block_in_chain = *rBlock;
                printf("[%d] Agregado a la lista bloque con index %d enviado por %d \n", mpi_rank, rBlock->index,status->MPI_SOURCE);
                return true;
            }else{
                //Si el índice del bloque recibido es
                //el siguiente a mí último bloque actual,
                //pero el bloque anterior apuntado por el recibido no es mí último actual,
                //entonces hay una blockchain más larga que la mía.
                printf("[%d] Perdí la carrera por uno (%d) contra %d \n", mpi_rank, rBlock->index, status->MPI_SOURCE);
                bool res = verificar_y_migrar_cadena(rBlock,status);
                return res;
            }
        }else if(last_block_in_chain->index >= rBlock->index){
            //-Si el índice del bloque recibido es igua al índice de mi último bloque actual,
            //entonces hay dos posibles forks de la blockchain pero mantengo la mía
            //-Si el índice del bloque recibido es anterior al índice de mi último bloque actual,
            //entonces lo descarto porque asumo que mi cadena es la que está quedando preservada.
            printf("[%d] Conflicto suave: Conflicto de branch (%d) contra %d \n",mpi_rank,rBlock->index,status->MPI_SOURCE);
            return false;    
        }else if(last_block_in_chain->index + 1 < rBlock->index){
            //Si el índice del bloque recibido está más de una posición adelantada a mi último bloque actual,
            //entonces me conviene abandonar mi blockchain actual
            printf("[%d] Perdí la carrera por varios contra %d \n", mpi_rank, status->MPI_SOURCE);
            bool res = verificar_y_migrar_cadena(rBlock,status);
            return res;
        }
    }
    printf("[%d] Error duro: Descarto el bloque recibido de %d porque no es válido \n",mpi_rank,status->MPI_SOURCE);
    return false;
}


//Envia el bloque minado a todos los nodos
void broadcast_block(const Block *block){
    MPI_Request m;
    for(int i = mpi_rank + 1; i < total_nodes; i++){
        MPI_Isend(block, 1, *MPI_BLOCK, i, TAG_NEW_BLOCK, MPI_COMM_WORLD,&m);
    }
    for(int i = 0; i < mpi_rank; i++){
        MPI_Isend(block, 1, *MPI_BLOCK, i, TAG_NEW_BLOCK, MPI_COMM_WORLD,&m);
    }
}

//Proof of work
//TODO: Advertencia: puede tener condiciones de carrera
void* proof_of_work(void *ptr){
    string hash_hex_str;
    Block block;
    unsigned int mined_blocks = 0;
    while(true){
        block = *last_block_in_chain;

        //Preparar nuevo bloque
        block.index += 1;
        block.node_owner_number = mpi_rank;
        block.difficulty = DEFAULT_DIFFICULTY;
        memcpy(block.previous_block_hash,block.block_hash,HASH_SIZE);

        //Agregar un nonce al azar al bloque para intentar resolver el problema
        gen_random_nonce(block.nonce);

        //Hashear el contenido (con el nuevo nonce)
        block_to_hash(&block,hash_hex_str);

        //Contar la cantidad de ceros iniciales (con el nuevo nonce)
        if(solves_problem(hash_hex_str)){
            printf("hola");
            //Verifico que no haya cambiado mientras calculaba
            if(last_block_in_chain->index < block.index){
                mined_blocks += 1;
                *last_block_in_chain = block;
                strcpy(last_block_in_chain->block_hash, hash_hex_str.c_str());
                last_block_in_chain->created_at = static_cast<unsigned long int> (time(NULL));
                node_blocks[hash_hex_str] = *last_block_in_chain;
                printf("[%d] Agregué un producido con index %d \n",mpi_rank,last_block_in_chain->index);

                //Mientras comunico, no responder mensajes de nuevos nodos
                pthread_mutex_lock(&broadcast_mutex); 
                broadcast_block(last_block_in_chain);
                pthread_mutex_unlock(&broadcast_mutex); 
            }
        }

    }

    return NULL;
}


int node(){

    //Tomar valor de mpi_rank y de nodos totales
    MPI_Comm_size(MPI_COMM_WORLD, &total_nodes);
    MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);

    //La semilla de las funciones aleatorias depende del mpi_ranking
    srand(time(NULL) + mpi_rank);
    printf("[MPI] Lanzando proceso %u\n", mpi_rank);

    last_block_in_chain = new Block;

    //Inicializo el primer bloque
    last_block_in_chain->index = 0;
    last_block_in_chain->node_owner_number = mpi_rank;
    last_block_in_chain->difficulty = DEFAULT_DIFFICULTY;
    last_block_in_chain->created_at = static_cast<unsigned long int> (time(NULL));
    memset(last_block_in_chain->previous_block_hash,0,HASH_SIZE);

    //Inicializo el broadcast mutex
    pthread_mutex_init(&broadcast_mutex,NULL);
    //Crear thread para minar
    pthread_t thread;
    pthread_create(&thread,NULL,proof_of_work,NULL);

    MPI_Status status;
    Block block;
    while(true){
        MPI_Probe(MPI_ANY_SOURCE,MPI_ANY_TAG,MPI_COMM_WORLD,&status);
        //TODO: Recibir mensajes de otros nodos
        if(status.MPI_TAG == TAG_NEW_BLOCK){
            MPI_Recv(&block,1,*MPI_BLOCK,status.MPI_SOURCE,TAG_NEW_BLOCK,MPI_COMM_WORLD,&status);
            pthread_mutex_lock(&broadcast_mutex);  
            validate_block_for_chain(&block, &status);
            pthread_mutex_unlock(&broadcast_mutex);  
        }else if(status.MPI_TAG ==TAG_CHAIN_HASH){

        }else if(status.MPI_TAG ==TAG_CHAIN_RESPONSE){

        }
    }

    //TODO: joinear el thread
    delete last_block_in_chain;
    return 0;
}