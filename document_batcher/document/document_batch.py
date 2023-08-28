from __future__ import annotations
import datetime
import math

from ..io.output_file import OutputFile
from .document import Document
from .document_batch_monitor import DocumentBatchMonitor


class DocumentBatch:

    def __init__(
        self        : DocumentBatch,
        monitor     : DocumentBatchMonitor,
        output_file : OutputFile
    ) -> DocumentBatch:
        
        assert isinstance(monitor, DocumentBatchMonitor)
        assert (
            isinstance(output_file, OutputFile)
            or output_file is None
        )
        
        self._monitor     = monitor     ; del monitor
        self._output_file = output_file ; del output_file
        
        
        self._list_of_docs = []
        self._done = False


    def _validate_list_of_docs(
        self : DocumentBatch
    ) -> None:

        assert self._done is False
        assert isinstance(self._list_of_docs, list)
        for doc in self._list_of_docs:
            assert isinstance(doc, Document)


    def get_list_of_docs(self : DocumentBatch) -> list[Document]:

        assert self._done is False
        self._validate_list_of_docs()

        return self._list_of_docs


    def get_len_in_docs(self : DocumentBatch) -> int:

        assert self._done is False
        self._validate_list_of_docs()

        return len(self._list_of_docs)

    
    def get_index(self : DocumentBatch) -> int:
        
        assert self._done is False
        assert isinstance(self._monitor, DocumentBatchMonitor)

        return self._monitor.get_current_doc_batch_index()


    def append_doc(
        self          : DocumentBatch,
        doc_to_append : Document
    ) -> None:

        # validate method-invocation preconditions
        assert self._done is False
        self._validate_list_of_docs()

        # validate method argument
        assert isinstance(doc_to_append, Document)


        self._list_of_docs.append(doc_to_append)


    def set_begin_datetime(
        self  : DocumentBatch,
        begin : datetime.datetime | str
    ) -> None:

        # validate method-invocation preconditions
        assert self._done is False

        # validate method argument
        assert isinstance(begin, datetime.datetime)
       
        
        self._list_of_docs[0].set_begin_doc_batch_datetime(begin)
        for doc in self._list_of_docs[1:]:
            doc.set_begin_doc_batch_datetime('not first doc in doc batch')


    def set_end_datetime(
        self : DocumentBatch,
        end  : datetime.datetime
    ) -> None:

        # validate method-invocation preconditions
        assert self._done is False

        # validate method argument
        assert isinstance(end, datetime.datetime)


        self._list_of_docs[-1].set_end_doc_batch_datetime(end)
        for doc in self._list_of_docs[:-1]:
            doc.set_end_doc_batch_datetime('not last doc in doc batch')

    
    def update_monitor(
        self : DocumentBatch
    ) -> None:
        
        # validate method-invocation preconditions
        assert self._done is False
        assert isinstance(self._monitor, DocumentBatchMonitor)
        self._validate_list_of_docs()
        
        
        n_docs  = 0
        n_sents = 0
        n_words = 0
        n_chars = 0

        for doc in self._list_of_docs:

            n_docs  += 1
            n_sents += doc.get_len_in_sents()
            n_words += doc.get_len_in_words()
            n_chars += doc.get_len_in_chars()


        begin = self._list_of_docs[0].get_begin_doc_batch_datetime()
        end   = self._list_of_docs[-1].get_end_doc_batch_datetime()
        
        n_secs = math.ceil((end - begin).total_seconds())
        
        self._monitor.post_batch_update(
            batch_n_docs  = n_docs,
            batch_n_sents = n_sents,
            batch_n_words = n_words,
            batch_n_chars = n_chars,
            batch_n_secs  = n_secs
        )


    def write_to_disk(
        self : DocumentBatch
    ) -> None:
        
        assert self._done is False
        assert isinstance(self._output_file, OutputFile)
        self._validate_list_of_docs()

        for doc in self._list_of_docs:
            self._output_file.append_output_doc(doc)


        self.update_monitor()
        self.set_done(True)


    def set_done(
        self : DocumentBatch,
        done : bool
    ) -> None:
        assert self._done is False
        assert done is True
        self._done = True
    
        
    def is_done(self : DocumentBatch) -> bool:
        assert isinstance(self._done, bool)
        return self._done


