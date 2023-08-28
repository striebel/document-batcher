from __future__ import annotations
import os
import json
from textwrap import dedent
from logging import Logger
from itertools import count
import pickle
import _io

from ..document.document import Document


class OutputFile:
    
    #######################################################
    ## logger
    
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
    ## predicted statistics key
    
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
    ## output file path
    
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
    ## from cache force
    
    def _set_from_cache_force(
        self             : OutputFile,
        from_cache_force : bool
    ) -> None:
        assert not hasattr(self, '_from_cache_force')
        assert isinstance(from_cache_force, bool)
        self._from_cache_force = from_cache_force
    
    def _is_from_cache_force(self : OutputFile) -> bool:
        assert isinstance(self._from_cache_force, bool)
        return self._from_cache_force
    
    
    #######################################################
    ## file path manipulations; cache file path
    
    def get_file_path_without_suffix(self : OutputFile) -> str:
        file_name = os.path.basename(self._get_file_path())
        suffix_index = file_name.rfind('.')
        if 0 < suffix_index: # we want to make the base file name have a length
                             # of at least one character
            base = file_name[:suffix_index]
        else:
            base = file_name
        return os.path.join(
            os.path.dirname(self._get_file_path()),
            base
        )
    
    def _get_cache_file_path(self : OutputFile) -> str:
        return \
            self.get_file_path_without_suffix() \
            + '.output_file_cache_v1.pickle'
    
    
    #######################################################
    ## output file cache
    
    def _set_read_only(
        self      : OutputFile,
        read_only : bool
    ) -> None:
        assert (
            not hasattr(self, '_read_only')
            or isinstance(self._read_only, bool)
        )
        assert isinstance(read_only, bool)
        if read_only:
            if hasattr(self, '_file'):
                assert isinstance(self._file, _io.TextIOWrapper)
                self._file.close()
                del self._file
        self._read_only = read_only
    
    def _is_read_only(self : OutputFile) -> bool:
        assert isinstance(self._read_only, bool)
        return self._read_only

    def _remove_cache(self : OutputFile) -> None:
        assert self._cache_is_available_on_disk()
        os.remove(self._get_cache_file_path())
    
    def _cache_is_available_on_disk(self : OutputFile) -> bool:
        if (
            os.path.isfile(self._get_cache_file_path())
            and 0 == os.path.getsize(self._get_cache_file_path())
        ):
            os.remove(self._get_cache_file_path())
            return False
        else:
            return os.path.isfile(self._get_cache_file_path())
    
    def _validate_cache(self : OutputFile) -> None:
        if __debug__:
            assert hasattr(self, '_cache')
            assert isinstance(self._cache, dict)
            assert 4 == len(self._cache)
            assert isinstance(self._cache['docs'], list)
            for doc in self._cache['docs']:
                assert isinstance(doc, Document)
            assert 0 <= self._cache['file_len_in_sents']
            assert isinstance(self._cache['file_len_in_words'], int)
            assert 0 <= self._cache['file_len_in_words']
            assert isinstance(self._cache['file_len_in_chars'], int)
            assert 0 <= self._cache['file_len_in_chars']
    
    def _init_cache(self : OutputFile) -> None:
        assert not hasattr(self, '_cache')
        self._cache = dict()
        self._cache['docs'] = list()
        self._cache['file_len_in_sents'] = 0
        self._cache['file_len_in_words'] = 0
        self._cache['file_len_in_chars'] = 0
        self._validate_cache()
        self._set_read_only(False)
    
    def _read_cache_from_disk(self : OutputFile) -> None:
        assert not hasattr(self, '_cache')
        assert self._cache_is_available_on_disk()
        cache_file = open(self._get_cache_file_path(), 'rb')
        self._cache = pickle.load(cache_file)
        cache_file.close()
        self._validate_cache()
        self._set_read_only(True)
    
    def write_cache_to_disk(self : OutputFile) -> None:
        assert not self._is_read_only()
        self._validate_cache()
        assert not self._cache_is_available_on_disk()
        cache_file = open(self._get_cache_file_path(), 'wb')
        pickle.dump(self._cache, cache_file)
        cache_file.close()
        self._set_read_only(True)

    def cache_exists(self : OutputFile) -> bool:
        return self._cache_is_available_on_disk()
    
    
    #######################################################
    ## update and get the length of the output file
    
    def get_file_len_in_docs(self : OutputFile) -> int:
        return len(self._cache['docs'])
    
    def _file_len_in_sents_plus_equals(
        self    : OutputFile,
        n_sents : int
    ) -> None:
        assert not self._is_read_only()
        assert isinstance(n_sents, int)
        self._cache['file_len_in_sents'] += n_sents
        assert 0 <= self._cache['file_len_in_sents']
        
    def get_file_len_in_sents(self : OutputFile) -> int:
        assert 0 <= self._cache['file_len_in_sents']
        return self._file_len_in_sents
    
    def _file_len_in_words_plus_equals(
        self    : OutputFile,
        n_words : int
    ) -> None:
        assert not self._is_read_only()
        assert isinstance(n_words, int)
        self._cache['file_len_in_words'] += n_words
        assert 0 <= self._cache['file_len_in_words']
    
    def get_file_len_in_words(self : OutputFile) -> int:
        assert 0 <= self._cache['file_len_in_words']
        return self._cache['file_len_in_words']
    
    def _file_len_in_chars_plus_equals(
        self    : OutputFile,
        n_chars : int
    ) -> None:
        assert not self._is_read_only()
        assert isinstance(n_chars, int)
        self._cache['file_len_in_chars'] += n_chars
        assert 0 <= self._cache['file_len_in_chars']

    def get_file_len_in_chars(self : OutputFile) -> int:
        assert 0 <= self._cache['file_len_in_chars']
        return self._cache['file_len_in_chars']
    
    
    #######################################################
    ## constructor
    
    def __init__(
        self                     : OutputFile,
        logger                   : Logger     = None,  # required
        file_path                : str        = None,  # required
        predicted_statistics_key : str        = None,  # required
        from_cache_force         : bool       = False  # optional
    ) -> OutputFile:
        self._set_logger(logger)                                    ; del logger
        self._set_file_path(file_path)                              ; del file_path
        self._set_predicted_statistics_key(predicted_statistics_key); del predicted_statistics_key
        self._set_from_cache_force(from_cache_force)                ; del from_cache_force
        
        self._get_logger().debug(
            'io.output_file: OutputFile.__init__: file_path: {0}'
            .format(self._get_file_path())
        )
        
        
        if (
            os.path.isfile(self._get_file_path())
            and 0 == os.path.getsize(self._get_file_path())
        ):
                os.remove(self._get_file_path())
        
        if (
            not os.path.isfile(self._get_file_path())
            and self._cache_is_available_on_disk()
        ):
            self._remove_cache()
        
        
        if (
            self._is_from_cache_force()
            or self._cache_is_available_on_disk()
        ):
            self._get_logger().debug(
                dedent(
                    '''\
                    io.output_file: OutputFile.__init__:
                    begin loading the cache,
                    i.e. the serialized (pickled) version
                    of already finalized output file'''
                ).replace('\n', ' ')
            )
            self._read_cache_from_disk()
            self._get_logger().debug(
                dedent(
                    '''\
                    io.output_file: OutputFile.__init__:
                    end loading the cache'''
                ).replace('\n', ' ')
            )
        else:
            self._get_logger().debug(
                dedent(
                    '''\
                    io.output_file: OutputFile.__init__:
                    initializing new cache'''
                ).replace('\n', ' ')
            )
            self._init_cache()
            self._sizes = [] # parallel list to the self._cache['docs'] list
                             #     where the entry at index i in self._sizes
                             #     is the byte-index plus one
                             #     of the last byte in the output file
                             #     which belongs to the document self._cache['docs'][i];
                             #     therefore truncating the output file to length
                             #     self._sizes[i] will leave an output file that contains
                             #     exactly the first i+1 documents
                             #     (considering that the lists are zero-indexed)
                             #     with all subsequent documents discarded
            if os.path.isfile(self._get_file_path()):
                assert 0 < os.path.getsize(self._get_file_path())

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
                        self._cache['docs'].append(
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
                    assert doc_index+1 == ndocs
            
            assert self.get_file_len_in_docs() == len(self._cache['docs'])
            assert self.get_file_len_in_docs() == len(self._sizes)
            
            for doc, size in zip(self._cache['docs'], self._sizes):
                assert isinstance(size, int)
                assert 0 < size
                self._file_len_in_sents_plus_equals(doc.get_len_in_sents())
                self._file_len_in_words_plus_equals(doc.get_len_in_words())
                self._file_len_in_chars_plus_equals(doc.get_len_in_chars())
            
            assert self.get_file_len_in_docs() == len(self._cache['docs'])
            assert self.get_file_len_in_docs() == len(self._sizes)
    
    
    #######################################################
    ## get a document in the output file by its index
    
    def get_output_doc_at_index(
        self      : OutputFile,
        doc_index : int
    ) -> Document:
        if 0 <= doc_index and doc_index < self.get_file_len_in_docs():
            return self._cache['docs'][doc_index]
        else:
            assert False, \
                'doc_index is {0} but should be in the interval [0, {1}]' \
                .format(
                    doc_index,
                    self.get_file_len_in_docs()-1
                )


    #################################################################
    ## truncate all data in the output file after a given document
    ##     specified by its document index;
    ##     this method may only be called once
    
    def delete_output_docs_after_index(
        self      : OutputFile,
        doc_index : int
    ) -> None:
        if self.get_file_len_in_docs() - 1 == doc_index:
            return
        
        assert not self._is_read_only()
        assert hasattr(self, '_sizes')  # check that the current method has not yet been called;
                                        #     (the _sizes attribute is deleted at the end
                                        #     of this method)
        assert self.get_file_len_in_docs() == len(self._sizes)
        
        if -1 == doc_index:
            if os.path.isfile(self._get_file_path()):
                os.remove(self._get_file_path())
        elif 0 <= doc_index and doc_index < self.get_file_len_in_docs() - 1:

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
            assert False, 'should have already returned'
        else:
            assert self.get_file_len_in_docs() <= doc_index
            assert False, \
                'doc_index is {0} but should be in the interval [-1, {1}]' \
                .format(
                    doc_index,
                    self.get_file_len_in_docs() - 1
                )
        
        for doc in self._cache['docs'][doc_index+1:]:
            self._file_len_in_sents_plus_equals(-doc.get_len_in_sents())
            self._file_len_in_words_plus_equals(-doc.get_len_in_words())
            self._file_len_in_chars_plus_equals(-doc.get_len_in_chars())
        
        self._cache['docs'] = self._cache['docs'][:doc_index+1]
        self._sizes         = self._sizes[:doc_index+1]
        
        assert self.get_file_len_in_docs() == doc_index + 1
        assert self.get_file_len_in_docs() == len(self._cache['docs'])
        assert self.get_file_len_in_docs() == len(self._sizes)
        
        del self._sizes # the _sizes attribute is used only by this method
                        #     and this method must only be called once,
                        #     so we now delete the _sizes attribute
        
    
    #######################################################
    ## open the output file in append mode

    def open_for_appending(self : OutputFile) -> None:
        assert not self._is_read_only()
        assert not hasattr(self, '_file')
        if os.path.isfile(self._get_file_path()):
            os.chmod(self._get_file_path(), 0o600)
        self._file = open(self._get_file_path(), 'a')
    
    
    #######################################################
    ## append a document to the output file
    
    def append_output_doc(
        self : OutputFile,
        doc  : Document
    ) -> None:

        # confirm that the output file is not yet finalized
        assert not self._is_read_only()
        
        # confirm that the output file is ready to be written;
        #     i.e., the delete_output_docs_after_index
        #     method has already been called
        assert hasattr(self, '_file')
        assert isinstance(self._file, _io.TextIOWrapper)
        assert 'a' == self._file.mode # 'a' for append
        
        
        json_str = json.dumps(
            doc.get_output_dict(
                predicted_statistics_key = self._get_predicted_statistics_key()
            )
        )
        if 0 < self._file.tell():
            json_str = '\n' + json_str
        
        self._file.write(json_str)
        
        self._cache['docs'].append(doc)

        self._file_len_in_sents_plus_equals(doc.get_len_in_sents())
        self._file_len_in_words_plus_equals(doc.get_len_in_words())
        self._file_len_in_chars_plus_equals(doc.get_len_in_chars())
        

