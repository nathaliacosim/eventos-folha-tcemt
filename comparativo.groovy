def leftJoin = { a, alias_a, key_a, b, alias_b, key_b ->
   def aliasLeituraTCE = b.groupBy{ it.with(key_b) }
   a.collect{ s->
       def foreigh = aliasLeituraTCE.get(s.with(key_a) )
       if(foreigh != null){
          foreigh.collect{ t->
               [(alias_a): s ,(alias_b): t ]
          }
       }
       else{
         [(alias_a): s ,(alias_b): null ]
       }
   }
   .flatten()
}

def printCsv = { l ->
   l.with{ asLista ->
         if(asLista?.size()>0){
           def cols = asLista.getAt(0).keySet()
           println(cols.collect{c->"$c"}.join(";"))
           asLista.each{ linha ->
             println(cols.collect{ c-> "${linha.get(c)}"}.join(";"))
           }
         }      
       }
}
def toMap = { f->
   new BufferedReader(new FileReader(new File(f)))
                       .readLines()
                       .collect({  it.split(";") })
                       .with{ l -> 
                              def c = l.getAt(0).collect{ c -> c.replace("\"","") };
                              def ret = (
                                l.collect{ r -> 
                                   ([:]).with{ o->
                                       r.eachWithIndex{ v, i ->
                                          o<< [(c.getAt(i)):v ] 
                                       }
                                       o
                                   }
                                }
                                )
                                (ret - ret.head())
                        }
}

def leituraCloud = toMap("C:\\Users\\DevStaf02\\Área de Trabalho\\EVENTOS\\eventosFolha.csv")
def leituraTCE   = toMap("C:\\Users\\DevStaf02\\Área de Trabalho\\EVENTOS\\tabelaTCE.csv")


def pesquisa = leftJoin(leituraCloud, "cloud", { t-> t.codigoTCE },
                        leituraTCE,"tce",{ t-> t.TDBG_CODIGO })

//pesquisa.each{ println it }

def eventosInconsistentes = pesquisa.findAll({ it.cloud?.naturezaEvento != it.tce?.TDBG_TIPO && it.tce })
       .collect{ row ->
           [:].with{ mapa ->
               STAF_CODIGO_EVENTO   = row.cloud?.codigoEvento
               STAF_TIPO_EVENTO     = row.cloud?.tipoEvento
               STAF_CODIGO_TCE      = row.cloud?.codigoTCE
               STAF_NATUREZA_EVENTO = row.cloud?.naturezaEvento
               TCE_TDBG_CODIGO      = row.tce?.TDBG_CODIGO
               TCE_TDBG_TIPO        = row.tce?.TDBG_TIPO
               DESCRICAO_ERRO       = "Existe uma inconsistencia entra os campos STAF_NATUREZA_EVENTO e TCE_TDBG_TIPO, favor realizar a correcao."
               
               (mapa)
           }
          } 
          
//printCsv(eventosInconsistentes)

def eventosNaoPreenchidos = pesquisa.findAll({ o -> ( o.tce == null || o.cloud.codigoTCE.contains("N") || o.cloud.naturezaEvento.contains("N") ) && o.cloud?.tipoEvento?.contains("VENCIMENTO") })
       .collect{ row ->
           [:].with{ mapa ->
               STAF_CODIGO_EVENTO   = row.cloud?.codigoEvento
               STAF_TIPO_EVENTO     = row.cloud?.tipoEvento
               STAF_CODIGO_TCE      = row.cloud?.codigoTCE.contains("N") ? "NAO INFORMADO" : row.cloud?.codigoTCE
               STAF_NATUREZA_EVENTO = row.cloud?.naturezaEvento.contains("N") ? "NAO INFORMADO" : row.cloud?.naturezaEvento
               DESCRICAO_ERRO       = "Existem evento que nao foram preenchidos corretamente."
               SOLUCAO_ERRO         = "Verifique os campos adicionais no cadastro de Eventos."
               IMPORTANTE           = "IGNORAR EVENTOS QUE COMPOEM A BASE DE CALCULO. NAO PREENCHER CAMPOS ADICIONAIS."               
               (mapa)
           }
          }
          
//printCsv(eventosNaoPreenchidos)
/*
join
   .findAll{ o-> o?.leituraCloud?.naturezaEvento != o?.leituraTCE?.TDBG_TIPO && o?.leituraTCE != null }
   .each{ println it } */
""