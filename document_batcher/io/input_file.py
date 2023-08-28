from __future__ import annotations
import os
import json
from textwrap import dedent
from logging import Logger
import pickle

from ..document.document import Document


class InputFile:
    
    #######################################################
    #### logger
    
    def _set_logger(
        self   : InputFile,
        logger : Logger
    ) -> None:
        assert not hasattr(self, '_logger')
        assert isinstance(logger, Logger)
        self._logger = logger
    
    def _get_logger(self : InputFile) -> Logger:
        assert isinstance(self._logger, Logger)
        return self._logger
    
    
    #######################################################
    #### input file path
    
    def _set_file_path(
        self      : InputFile,
        file_path : str
    ) -> None:
        assert not hasattr(self, '_file_path')
        assert isinstance(file_path, str)
        assert os.path.isfile(file_path)
        self._file_path = file_path
    
    def _get_file_path(
        self : InputFile
    ) -> None:
        assert isinstance(self._file_path, str)
        assert os.path.isfile(self._file_path)
        return self._file_path
    
    
    #######################################################
    #### file path manipulations; cache file path
    
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
    
    def _get_cache_file_path(self : InputFile) -> str:
        return \
            self.get_file_path_without_suffix() \
            + '.input_file_cache_v1.pickle'
    
    
    #######################################################
    #### enforced maximum sentence length in characters
    
    def _set_max_sent_len_in_chars(
        self                  : InputFile,
        max_sent_len_in_chars : int
    ) -> None:
        assert hasattr(self, '_max_sent_len_in_chars') is False
        assert isinstance(max_sent_len_in_chars, int)
        assert 0 < max_sent_len_in_chars
        self._max_sent_len_in_chars = max_sent_len_in_chars
        
    def _get_max_sent_len_in_chars(
        self : InputFile
    ) -> int:
        assert isinstance(self._max_sent_len_in_chars, int)
        assert 0 < self._max_sent_len_in_chars
        return self._max_sent_len_in_chars
    
    
    #######################################################
    #### constructor
    
    def __init__(
        self                     : InputFile,
        logger                   : Logger     = None,                   # required
        file_path                : str        = None,                   # required
        max_sent_len_in_chars    : int        = 2048,                   # optional
    ) -> InputFile:
        
        self._set_logger(logger)                                    ; del logger
        self._set_file_path(file_path)                              ; del file_path
        self._set_max_sent_len_in_chars(max_sent_len_in_chars)      ; del max_sent_len_in_chars
        
        self._get_logger().debug(
            'io.input_file: InputFile.__init__: file_path: {0}'
            .format(
                self._get_file_path()
            )
        )
        
        
        #########################################
        ## load the cache file if it exists;
        ## otherwise, build it
        
        if (
            os.path.isfile(self._get_cache_file_path())
            and 0 == os.path.getsize(self._get_cache_file_path())
        ):
            os.remove(self._get_cache_file_path())
        
        self._cache = {}
        
        if os.path.isfile(self._get_cache_file_path()) is True:

            self._get_logger().debug(
                dedent(
                    '''\
                    io.input_file: InputFile.__init__:
                    load input file cache: begin'''
                ).replace('\n', ' ')
            )
            
            with open(self._get_cache_file_path(), 'rb') as cache_file:
                self._cache = pickle.load(cache_file)
                del cache_file
            assert isinstance(self._cache, dict)
            assert 0 < len(self._cache)
            
            self._get_logger().debug(
                dedent(
                    '''\
                    io.input_file: InputFile.__init__:
                    load input file cache: end'''
                ).replace('\n', ' ')
            )
            
        else:
            
            assert os.path.isfile(self._get_cache_file_path()) is False
            
            self._get_logger().debug(
                dedent(
                    '''\
                    io.input_file: InputFile.__init__:
                    read the input file into memory: begin'''
                ).replace('\n', ' ')
            )
            
            doc_dicts = []
            with open(self._get_file_path(), 'r') as file:
                doc_dicts = [
                    json.loads(doc_json_str)
                    for doc_json_str
                    in file.read().strip().splitlines()
                ]
            assert 0 < len(doc_dicts)
            
            self._get_logger().debug(
                dedent(
                    '''\
                    io.input_file: InputFile.__init__:
                    read the input file into memory: end'''
                ).replace('\n', ' ')
            )
            
            self._get_logger().debug(
                dedent(
                    '''\
                    io.input_file: InputFile.__init__:
                    generate input file cache: begin'''
                ).replace('\n', ' ')
            )
            
            self._cache['docs'] = []
            self._cache['file_len_in_sents'] = 0
            self._cache['file_len_in_words'] = 0
            self._cache['file_len_in_chars'] = 0
            
            for doc_index, doc_dict in enumerate(doc_dicts):
                
                if (
                    0 == doc_index
                    or 0 == (doc_index+1)%100
                    or doc_index+1 == len(doc_dicts)
                ):
                    self._get_logger().debug(
                        dedent(
                            '''\
                            io.input_file: InputFile.__init__:
                            initializing input doc {0} (of {1})'''
                        ).replace('\n', ' ').format(
                            doc_index + 1,
                            len(doc_dicts)
                        )
                    )

                doc = Document.from_input_doc_dict(
                    logger                   = self._get_logger(),
                    input_doc_dict           = doc_dict,
                    max_sent_len_in_chars    = self._get_max_sent_len_in_chars()
                )
                
                self._cache['docs'].append(doc)
                
                self._cache['file_len_in_sents'] += doc.get_len_in_sents()
                self._cache['file_len_in_words'] += doc.get_len_in_words()
                self._cache['file_len_in_chars'] += doc.get_len_in_chars()
                
            
            self._get_logger().debug(
                dedent(
                    '''\
                    io.input_file: InputFile.__init__:
                    generate input file cache: end'''
                ).replace('\n', ' ')
            )
            
            self._get_logger().debug(
                dedent(
                    '''\
                    io.input_file: InputFile.__init__:
                    write input file cache to file: begin'''
                ).replace('\n', ' ')
            )
            
            with open(self._get_cache_file_path(), 'wb') as cache_file:
                pickle.dump(self._cache, cache_file)
                del cache_file

            self._get_logger().debug(
                dedent(
                    '''\
                    io.input_file: InputFile.__init__:
                    write input file cache to file: end'''
                ).replace('\n', ' ')
            )


        self._get_logger().debug(
            'io.input_file: InputFile.__init__: file_len_in_docs : {0}'
            .format(
                self.get_file_len_in_docs()
            )
        )
        self._get_logger().debug(
            'io.input_file: InputFile.__init__: file_len_in_sents: {0}'
            .format(
                self.get_file_len_in_sents()
            )
        )
        self._get_logger().debug(
            'io.input_file: InputFile.__init__: file_len_in_words: {0}'
            .format(
                self.get_file_len_in_words()
            )
        )
        self._get_logger().debug(
            'io.input_file: InputFile.__init__: file_len_in_chars: {0}'
            .format(
                self.get_file_len_in_chars()
            )
        )

        self._next_doc_index = 0
    
    
    #######################################################
    #### get file length statistics
    
    def get_file_len_in_docs(self : InputFile) -> int:
        return len(self._cache['docs'])
    
    def get_file_len_in_sents(self : InputFile) -> int:
        assert isinstance(self._cache['file_len_in_sents'], int)
        assert 0 <= self._cache['file_len_in_sents']
        return self._cache['file_len_in_sents']
    
    def get_file_len_in_words(self : InputFile) -> int:
        assert isinstance(self._cache['file_len_in_words'], int)
        assert 0 <= self._cache['file_len_in_words']
        return self._cache['file_len_in_words']
    
    def get_file_len_in_chars(self : InputFile) -> int:
        assert isinstance(self._cache['file_len_in_chars'], int)
        assert 0 <= self._cache['file_len_in_chars']
        return self._cache['file_len_in_chars']


    #######################################################
    #### read documents from the file,
    ####     which has already been loaded into the cache
    
    def get_next_input_doc(self : InputFile) -> Document:
        assert 0 <= self._next_doc_index
        assert self._next_doc_index < self.get_file_len_in_docs()
        input_doc = self._cache['docs'][self._next_doc_index]
        self._next_doc_index += 1
        return input_doc
    
    def has_next_input_doc(self : InputFile) -> bool:
        return self._next_doc_index < self.get_file_len_in_docs()
    
    def set_next_input_doc_index(
        self      : InputFile,
        doc_index : int
    ) -> None:
        assert 0 < self._next_doc_index
        assert not hasattr(self, '_set_next_input_doc_index_has_occurred')
        self._set_next_input_doc_index_has_occurred = True
        assert 0 <= doc_index
        assert doc_index < self.get_file_len_in_docs() - 1
        self._next_doc_index = doc_index



