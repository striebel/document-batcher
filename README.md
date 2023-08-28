# document-batcher

Install with
```sh
$ pip install document-batcher
```

Example usage:
```py
import argparse
import logging
import datetime
import sys

from document_batcher.io.input_file import InputFile
from document_batcher.io.output_file import OutputFile
from document_batcher.document.document_batch_iterator import DocumentBatchIterator


PREDICTED_STATISTICS_KEY = 'character_frequencies'


def process_documents(doc_batch_iterator):
    
    begin = datetime.datetime.now(datetime.timezone.utc)
    
    char_to_freq = dict()
    
    for doc_batch in doc_batch_iterator:
    
        for doc in doc_batch.get_list_of_docs():
            
            for char in doc.get_full_text():
                
                if char not in char_to_freq:
                    char_to_freq[char] = 1
                else:
                    char_to_freq[char] += 1
            
            doc.set_predicted_statistics(char_to_freq)
            
        doc_batch.set_begin_datetime(begin)
        
        end = datetime.datetime.now(datetime.timezone.utc)
        
        doc_batch.set_end_datetime(end)
        
        begin = end
        
        doc_batch.write_to_disk()
    
    return 0 # success
         

if '__main__' == __name__:
    
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument(
        'input_file_path',
        action='store',
        type=str
    )
    arg_parser.add_argument(
        'output_file_path',
        action='store',
        type=str
    )
    arg_parser.add_argument(
        '--doc-batch-size',
        dest='doc_batch_size',
        action='store',
        type=int,
        default=8
    )
    args = arg_parser.parse_args()
    
    logging.basicConfig(
        format='%(asctime)s: %(levelname)s: %(name)s: %(message)s',
        datefmt='%Y-%m-%dT%H:%M:%S'
    )
    logger = logging.getLogger('character_frequencies_app')
    logger.setLevel(10) # DEBUG
    
    input_file = InputFile(
        logger    = logger,
        file_path = args.input_file_path
    )
    output_file = OutputFile(
        logger                   = logger,
        file_path                = args.output_file_path,
        predicted_statistics_key = PREDICTED_STATISTICS_KEY
    )
    doc_batch_iterator = DocumentBatchIterator(
        logger         = logger,
        input_file     = input_file,
        output_file    = output_file,
        doc_batch_size = args.doc_batch_size
    )
    
    retcode = process_documents(doc_batch_iterator)
    
    if 0 == retcode and not output_file.cache_exists():
        output_file.write_cache_to_disk()
    
    sys.exit(retcode)
```
