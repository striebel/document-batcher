from __future__ import annotations
from logging import Logger
from textwrap import dedent
import datetime

from .document import Document
from .document_batch import DocumentBatch
from .document_batch_monitor import DocumentBatchMonitor
from ..io.input_file import InputFile
from ..io.output_file import OutputFile


class DocumentBatchIterator:
    
    def __iter__(
        self : DocumentBatchIterator
    ) -> DocumentBatchIterator:

        return self
    
    
    def __next__(
        self : DocumentBatchIterator
    ) -> DocumentBatch:
        
        assert isinstance(self._doc_batch_size, int)
        assert 0 < self._doc_batch_size
        assert isinstance(self._input_file, InputFile)
        assert isinstance(self._output_file, OutputFile)
        assert isinstance(self._monitor, DocumentBatchMonitor)
        
        
        doc_batch = DocumentBatch(
            monitor     = self._monitor,
            output_file = self._output_file
        )
        
        for doc_batch_index in range(self._doc_batch_size):

            if self._input_file.has_next_input_doc():

                input_doc = self._input_file.get_next_input_doc()

                assert isinstance(input_doc, Document)

                doc_batch.append_doc(input_doc)

            else:

                break

        if 0 < doc_batch.get_len_in_docs():

            return doc_batch

        else:

            raise StopIteration
    
    
    def __init__(
        self           : DocumentBatchIterator,
        logger         : Logger               = None, # required
        input_file     : InputFile            = None, # required
        output_file    : OutputFile           = None, # required
        monitor        : DocumentBatchMonitor = None, # optional
        doc_batch_size : int                  = 8     # optional
    ) -> DocumentBatchIterator:
        assert isinstance(logger, Logger) 
        assert isinstance(input_file, InputFile)
        assert isinstance(output_file, OutputFile)
        if monitor is None:
            monitor = DocumentBatchMonitor(
                logger     = logger,
                input_file = input_file
            )
        else:
            assert isinstance(monitor, DocumentBatchMonitor)
        assert isinstance(doc_batch_size, int)
        assert 0 < doc_batch_size
        
        self._logger         = logger         ; del logger
        self._input_file     = input_file     ; del input_file
        self._output_file    = output_file    ; del output_file
        self._monitor        = monitor        ; del monitor
        self._doc_batch_size = doc_batch_size ; del doc_batch_size
        
        
        doc_batch = None
        end_doc_batch_index = -1
        
        doc_index = -1
        for doc_index in range(self._output_file.get_file_len_in_docs()):
            
            output_doc = self._output_file.get_output_doc_at_index(doc_index)
            assert isinstance(output_doc, Document)
            
            assert self._input_file.has_next_input_doc()
            input_doc = self._input_file.get_next_input_doc()
            assert isinstance(input_doc, Document)
            
            assert output_doc.get_id() == input_doc.get_id()
            
            
            begin_datetime = output_doc.get_begin_doc_batch_datetime()
            
            if isinstance(begin_datetime, datetime.datetime):
                
                assert doc_batch is None
                
                doc_batch = DocumentBatch(
                    monitor     = self._monitor,
                    output_file = None
                )

            else:

                assert doc_batch is not None

                assert 'not first doc in doc batch' == begin_datetime
            
            doc_batch.append_doc(output_doc)
            
            end_datetime = output_doc.get_end_doc_batch_datetime()
            
            if isinstance(end_datetime, datetime.datetime):

                assert doc_batch is not None

                doc_batch.update_monitor()
                doc_batch.set_done(True)

                doc_batch = None

                end_doc_batch_index = doc_index

            else:

                assert doc_batch is not None

                assert 'not last doc in doc batch' == end_datetime
            
        
        # Check whether writing the last doc batch in the output file was
        # interrupted which would cause the last doc in the output file
        # to have its end_doc_batch_datetime field set to
        # 'not last doc in doc batch'
        if doc_batch is not None:

            assert doc_batch.is_done() is False

            assert 'not last doc in doc batch' == end_datetime

            assert end_doc_batch_index < doc_index

            self._logger.error(
                dedent(
                    '''\
                    document.document_batch_iterator:
                    DocumentBatchIterator.__init__:
                    the existing output file is corrupted:
                    the last doc-result json line in the output file
                    does not contain an end doc batch datetime:
                    rather, the last line that has one is at index {0},
                    while the last line in the output file is at index {1}:
                    will now truncate the output file after the line at the former index,
                    rewind the input file to the former index plus one,
                    and attempt to restart processing'''
                ).replace('\n', ' ').format(
                    end_doc_batch_index,
                    doc_index
                )
            )
            
            self._input_file.set_next_input_doc_index(end_doc_batch_index + 1)
            
        else: # The last doc in the output file does end a doc batch as expected

            assert doc_batch is None

            if -1 < doc_index: # if the output file contains more than zero
                               # document results
                assert isinstance(end_datetime, datetime.datetime)

            assert end_doc_batch_index == doc_index
        
        
        self._output_file.delete_output_docs_after_index(end_doc_batch_index)

        if (
            self._output_file.get_file_len_in_docs()
            < self._input_file.get_file_len_in_docs()
        ):
            self._output_file.open_for_appending()

