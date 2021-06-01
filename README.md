# MAP-REDUCE HADOOP
Analisi delle recensioni di Amazon classificandole con un punteggio, media punti delle parole utilizzate.
Il punteggio di ogni singola parola si trova su SentiWordNet.txt

# Primo Approccio :
E' stato utilizzato il paradigma di programmazione MapRed in Java.

 - ONE JOB : in cui il pre-processing del SentiWordNet viene implementato con gli ‘strumenti tradizionali’ del java e salvato in cache come un oggetto «HashMap».
 
 - TWO JOB : in cui vengono eseguiti in cascata il primo Map-Reduce sul file SentiWordNet, e il secondo relativo alle Reviews, lanciati sequenzialmente nel Main.

# Secondo Approccio :
E' stato utilizzato PySpark su Hadoop.


