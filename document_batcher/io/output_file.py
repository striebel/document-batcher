from __future__ import annotations
import os
import json
from textwrap import dedent
from logging import Logger
from itertools import count

from ..document.document import Document


class OutputFile:


    #######################################################
    #### logger

    def _set_logger(
        self   : OutputFile,
        logger : Logger
    ) -> None:
        assert hasattr(self, '_logger') is False
        assert isinstance(logger, Logger)
        self._logger = logger

    def _get_logger(
        self : OutputFile
    ) -> Logger:
        assert isinstance(self._logger, Logger)
        return self._logger
    
    
    #######################################################
    #### predicted statistics key
    
    def _set_predicted_statistics_key(
        self                     : OutputFile,
        predicted_statistics_key : str
    ) -> None:
        assert not hasattr(self, '_predicted_statistics_key')
        assert isinstance(predicted_statistics_key, str)
        self._predicted_statistics_key = predicted_statistics_key
    
    def _get_predicted_statistics_key(self : OutputFile) -> str:
        assert isinstance(self._predicted_statistics_key, str)
        return self._predicted_statistics_key
    
    
    #######################################################
    #### output file path
    
    def _set_file_path(
        self      : OutputFile,
        file_path : str
    ) -> None:
        assert not hasattr(self, '_file_path')
        assert isinstance(file_path, str)
        self._file_path = file_path
    
    def _get_file_path(
        self : OutputFile
    ) -> str:
        assert isinstance(self._file_path, str)
        return self._file_path
    

    #######################################################
    #### constructor

    def __init__(
        self                     : OutputFile,
        logger                   : Logger,
        predicted_statistics_key : str,
        file_path                : str
    ) -> OutputFile:

        self._set_logger(logger)                                    ; del logger
        self._set_predicted_statistics_key(predicted_statistics_key); del predicted_statistics_key
        self._set_file_path(file_path)                              ; del file_path
        
        self._get_logger().debug(
            'io.output_file: OutputFile.__init__: file_path: {0}'
            .format(self._get_file_path())
        )
        
        self._docs = []  # list of Document objects in the output file

        self._sizes = [] # parallel list where the entry at index n
                         #     is the byte-index plus one
                         #     of the last byte in the output file
                         #     which belongs to the
                         #     document in the self._docs list at index n;
                         #     therefore truncating the output file to length
                         #     self._sizes[n] would leave an output file that contains
                         #     exactly the first n documents with all subsequent
                         #     documents discarded
        
        if (
            os.path.isfile(self._get_file_path())
            and 0 < os.path.getsize(self._get_file_path())
        ):
            with open(self._get_file_path(), 'rt') as output_file_tmode:
                ndocs = output_file_tmode.read().count('\n') + 1
            with open(self._get_file_path(), 'rb') as output_file_bmode:
                size = 0
                for doc_index in count():
                    json_bytes = b''
                    while True:
                        read_byte = output_file_bmode.read(1)
                        size += len(read_byte)
                        if read_byte in [b'', '\n'.encode('utf-8')]:
                            break
                        assert 1 == len(read_byte)
                        json_bytes += read_byte
                    assert 0 < len(json_bytes)
                    json_str = json_bytes.decode('utf-8')
                    if (
                        0 == doc_index
                        or 0 == (doc_index+1)%100
                        or doc_index+1 == ndocs
                    ):
                        self._get_logger().debug(
                            dedent(
                                '''\
                                io.output_file: OutputFile.__init__:
                                initializing output doc {0} (of {1})'''
                            ).replace('\n', ' ').format(
                                doc_index + 1,
                                ndocs
                            )
                        )
                    self._docs.append(
                        Document.from_output_doc_dict(
                            logger                   = self._get_logger(),
                            output_doc_dict          = json.loads(json_str),
                            predicted_statistics_key = self._get_predicted_statistics_key()
                        )
                    )
                    self._sizes.append(
                        size if b'' == read_byte else
                        size-1 if '\n'.encode('utf-8') == read_byte else
                        None
                    )
                    if b'' == read_byte:
                        assert b'' == output_file_bmode.read() # reconfirm we are at end of file
                        break

        self._file_len_in_docs  = 0
        self._file_len_in_sents = 0
        self._file_len_in_words = 0
        self._file_len_in_chars = 0
        
        assert len(self._docs) == len(self._sizes)
        for doc, size in zip(self._docs, self._sizes):
            assert isinstance(size, int)
            assert 0 < size
            self._file_len_in_docs_plus_equals(1)
            self._file_len_in_sents_plus_equals(doc.get_len_in_sents())
            self._file_len_in_words_plus_equals(doc.get_len_in_words())
            self._file_len_in_chars_plus_equals(doc.get_len_in_chars())

        assert self.get_file_len_in_docs() == len(self._docs)
        assert self.get_file_len_in_docs() == len(self._sizes)
    
    
    #######################################################
    #### get a document in the output file by its index;
    ####     this method cannot be called after the call
    ####     has been made to the below method,
    ####     delete_output_docs_after_index

    def get_output_doc_at_index(
        self      : OutputFile,
        doc_index : int
    ) -> Document:

        if 0 <= doc_index and doc_index < len(self._docs):
            return self._docs[doc_index]
        else:
            assert False, \
                'doc_index is {0} but should be in the interval [0, {1}]' \
                .format(
                    doc_index,
                    len(self._docs)-1
                )


    #################################################################
    #### truncate all data in the output file after a given document
    ####     specified by its document index
    ####     (not by byte index in the file);
    ####     this method may only be called once
    
    def delete_output_docs_after_index(
        self      : OutputFile,
        doc_index : int
    ) -> None:
        assert hasattr(self, '_docs')   # i.e., these two asserts confirm that
        assert hasattr(self, '_sizes')  # this method has not yet been called
                                        # (this method should only be called once)
        
        if -1 == doc_index:
            if os.path.isfile(self._get_file_path()):
                os.remove(self._get_file_path())
        elif 0 <= doc_index and doc_index < self.get_file_len_in_docs() - 1:
            assert self.get_file_len_in_docs() == len(self._sizes)
            assert self.get_file_len_in_docs() == len(self._docs)
            
            ## debugging code
            #import sys
            #print('doc_index             :', doc_index, file=sys.stderr)
            #print('self._sizes[doc_index]:', self._sizes[doc_index], file=sys.stderr)
            #print('self._sizes[22]       :', self._sizes[22], file=sys.stderr)
            #sys.exit(-1)
            
            with open(self._get_file_path(), 'r+b') as output_file:
                output_file.truncate(self._sizes[doc_index])
                del output_file
        elif self.get_file_len_in_docs() - 1 == doc_index:
            pass
        else:
            assert self.get_file_len_in_docs() <= doc_index
            assert False, \
                'doc_index is {0} but should be in the interval [-1, {1}]' \
                .format(
                    doc_index,
                    self.get_file_len_in_docs() - 1
                )
        
        for doc in self._docs[doc_index+1:]:
            self._file_len_in_docs_plus_equals(-1)
            self._file_len_in_sents_plus_equals(-doc.get_len_in_sents())
            self._file_len_in_words_plus_equals(-doc.get_len_in_words())
            self._file_len_in_chars_plus_equals(-doc.get_len_in_chars())

        self._docs  = self._docs[:doc_index+1]
        self._sizes = self._sizes[:doc_index+1]
        
        assert self.get_file_len_in_docs() == len(self._docs)
        assert self.get_file_len_in_docs() == len(self._sizes)
        
        del self._docs
        del self._sizes
        
        assert hasattr(self, '_file') is False
        self._file = open(self._get_file_path(), 'a')
    
   
    #######################################################
    #### append a document to the output file
    #### (writing it out to the file)

    def append_output_doc(
        self : Writer,
        doc  : Document
    ) -> None:
        assert 'a' == self._file.mode # 'a' stands for append

        json_str = json.dumps(doc.get_output_dict())
        if 0 < self._file.tell():
            json_str = '\n' + json_str

        self._file.write(json_str)

        self._file_len_in_docs_plus_equals(1)
        self._file_len_in_sents_plus_equals(doc.get_len_in_sents())
        self._file_len_in_words_plus_equals(doc.get_len_in_words())
        self._file_len_in_chars_plus_equals(doc.get_len_in_chars())
        
    
    #######################################################
    #### updated and get the length of output file

    def _file_len_in_docs_plus_equals(
        self  : OutputFile,
        ndocs : int
    ) -> None:
        assert isinstance(self._file_len_in_docs, int)
        assert isinstance(ndocs, int)
        self._file_len_in_docs += ndocs
        assert 0 <= self._file_len_in_docs
    
    def get_file_len_in_docs(self : OutputFile) -> int:
        assert isinstance(self._file_len_in_docs, int)
        assert 0 <= self._file_len_in_docs
        return self._file_len_in_docs

    def _file_len_in_sents_plus_equals(
        self    : OutputFile,
        n_sents : int
    ) -> None:
        assert isinstance(self._file_len_in_sents, int)
        assert isinstance(n_sents, int)
        self._file_len_in_sents += n_sents
        assert 0 <= self._file_len_in_sents

    def get_file_len_in_sents(self : OutputFile) -> int:
        assert isinstance(self._file_len_in_sents, int)
        assert 0 <= self._file_len_in_sents
        return self._file_len_in_sents

    def _file_len_in_words_plus_equals(
        self    : OutputFile,
        n_words : int
    ) -> None:
        assert isinstance(self._file_len_in_words, int)
        assert isinstance(n_words, int)
        self._file_len_in_words += n_words
        assert 0 <= self._file_len_in_words

    def get_file_len_in_words(self : OutputFile) -> int:
        assert isinstance(self._file_len_in_words, int)
        assert 0 <= self._file_len_in_words
        return self._file_len_in_words
    
    def _file_len_in_chars_plus_equals(
        self    : OutputFile,
        n_chars : int
    ) -> None:
        assert isinstance(self._file_len_in_chars, int)
        assert isinstance(n_chars, int)
        self._file_len_in_chars += n_chars
        assert 0 <= self._file_len_in_chars

    def get_file_len_in_chars(self : OutputFile) -> int:
        assert isinstance(self._file_len_in_chars, int)
        assert 0 <= self._file_len_in_chars
        return self._file_len_in_chars


