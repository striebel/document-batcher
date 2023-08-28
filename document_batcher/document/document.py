from __future__ import annotations
import sys
from logging import Logger
from textwrap import dedent
import datetime

import nltk # for sentence segmentation


class Document:
    
    
    #######################################################
    #### document id
    
    _ID_KEYS = ['document_id', 'documentID']
    
    @staticmethod
    def _get_id_keys() -> str:
        return Document._ID_KEYS.copy()
    
    def _set_id_key_to_use(
        self          : Document,
        id_key_to_use : str
    ) -> None:
        assert not hasattr(self, '_id_key_to_use')
        assert isinstance(id_key_to_use, str)
        assert id_key_to_use in Document._ID_KEYS
        self._id_key_to_use = id_key_to_use
    
    def _get_id_key_to_use(self : Document) -> str:
        assert isinstance(self._id_key_to_use, str)
        assert self._id_key_to_use in Document._ID_KEYS
        return self._id_key_to_use
    
    def _set_id(
        self   : Document,
        doc_id : str
    ) -> None:
        assert not hasattr(self, '_id')
        assert isinstance(doc_id, str)
        self._id = doc_id
    
    def get_id(self : Document) -> str:
        assert isinstance(self._id, str)
        return self._id
    
    
    #######################################################
    #### document text
    
    _TEXT_KEYS = ['fullText']
    
    @staticmethod
    def _get_text_keys() -> str:
        return Document._TEXT_KEYS.copy()

    def _set_full_text(
        self      : Document,
        full_text : str
    ) -> None:
        assert not hasattr(self, '_full_text')
        assert isinstance(full_text, str)
        self._full_text = full_text
    
    def get_full_text(self : Document) -> str:
        assert isinstance(self._full_text, str)
        return self._full_text
    
    def _set_list_of_sents(
        self          : Document,
        list_of_sents : list[str]
    ) -> None:
        if __debug__:
            assert not hasattr(self, '_list_of_sents')
            assert isinstance(list_of_sents, list)
            for sent in list_of_sents:
                assert isinstance(sent, str)
        self._list_of_sents = list_of_sents
    
    def get_list_of_sents(self : Document) -> list[str]:
        if __debug__:
            assert isinstance(self._list_of_sents, list)
            for sent in self._list_of_sents:
                assert isinstance(sent, str)
        return self._list_of_sents
    
    
    #######################################################
    #### document length in sentences
    
    _LEN_IN_SENTS_KEY = 'len_in_sents'

    @staticmethod
    def _get_len_in_sents_key():
        return Document._LEN_IN_SENTS_KEY
    
    def _set_len_in_sents(
        self         : Document,
        len_in_sents : int
    ) -> None:
        assert not hasattr(self, '_len_in_sents')
        assert isinstance(len_in_sents, int)
        assert 0 < len_in_sents
        self._len_in_sents = len_in_sents
    
    def get_len_in_sents(self : Document) -> int:
        assert isinstance(self._len_in_sents, int)
        assert 0 < self._len_in_sents
        return self._len_in_sents
   

    #######################################################
    #### document length in words

    _LEN_IN_WORDS_KEY = 'len_in_words'

    @staticmethod
    def _get_len_in_words_key():
        return Document._LEN_IN_WORDS_KEY

    def _set_len_in_words(
        self         : Document,
        len_in_words : int
    ) -> None:
        assert not hasattr(self, '_len_in_words')
        assert isinstance(len_in_words, int)
        assert 0 < len_in_words
        self._len_in_words = len_in_words

    def get_len_in_words(self : Document) -> int:
        assert isinstance(self._len_in_words, int)
        assert 0 < self._len_in_words
        return self._len_in_words
   

    #######################################################
    #### document length in characters

    _LEN_IN_CHARS_KEY = 'len_in_chars'

    @staticmethod
    def _get_len_in_chars_key():
        return Document._LEN_IN_CHARS_KEY

    def _set_len_in_chars(
        self         : Document,
        len_in_chars : int
    ) -> None:
        assert not hasattr(self, '_len_in_chars')
        assert isinstance(len_in_chars, int)
        assert 0 < len_in_chars
        self._len_in_chars = len_in_chars

    def get_len_in_chars(self : Document) -> int:
        assert isinstance(self._len_in_chars, int)
        assert 0 < self._len_in_chars
        return self._len_in_chars
    
    
    #######################################################
    #### document batch begin datetime

    _BEGIN_DOC_BATCH_DATETIME_KEY = 'begin_doc_batch_datetime'

    @staticmethod
    def _get_begin_doc_batch_datetime_key():
        return Document._BEGIN_DOC_BATCH_DATETIME_KEY

    def set_begin_doc_batch_datetime(
        self  : Document,
        begin : datetime.datetime | str
    ) -> None:
        assert not hasattr(self, '_begin_doc_batch_datetime')
        assert (
            isinstance(begin, datetime.datetime)
            or 'not first doc in doc batch' == begin
        )
        self._begin_doc_batch_datetime = begin
    
    def get_begin_doc_batch_datetime(
        self : Document
    ) -> datetime.datetime | str:
        assert (
            isinstance(self._begin_doc_batch_datetime, datetime.datetime)
            or 'not first doc in doc batch'  == self._begin_doc_batch_datetime
        )
        return self._begin_doc_batch_datetime
    
    def _begin_doc_batch_datetime_to_str(
        self : Document
    ) -> str:
        if isinstance(self._begin_doc_batch_datetime, datetime.datetime):
            return self._begin_doc_batch_datetime.isoformat()
        else:
            assert isinstance(self._begin_doc_batch_datetime, str)
            assert 'not first doc in doc batch' == self._begin_doc_batch_datetime
            return self._begin_doc_batch_datetime
    
    
    #######################################################
    #### document batch end datetime

    _END_DOC_BATCH_DATETIME_KEY = 'end_doc_batch_datetime'

    @staticmethod
    def _get_end_doc_batch_datetime_key():
        return Document._END_DOC_BATCH_DATETIME_KEY

    def set_end_doc_batch_datetime(
        self : Document,
        end  : datetime.datetime | str
    ) -> None:
        assert not hasattr(self, '_end_doc_batch_datetime')
        assert (
            isinstance(end, datetime.datetime)
            or 'not last doc in doc batch' == end
        )
        self._end_doc_batch_datetime = end

    def get_end_doc_batch_datetime(
        self : Document
    ) -> datetime.datetime | str:
        assert (
            isinstance(self._end_doc_batch_datetime, datetime.datetime)
            or 'not last doc in doc batch' == self._end_doc_batch_datetime
        )
        return self._end_doc_batch_datetime
    
    def _end_doc_batch_datetime_to_str(
        self : Document
    ) -> str:
        if isinstance(self._end_doc_batch_datetime, datetime.datetime):
            return self._end_doc_batch_datetime.isoformat()
        else:
            assert isinstance(self._end_doc_batch_datetime, str)
            assert 'not last doc in doc batch' == self._end_doc_batch_datetime
            return self._end_doc_batch_datetime
    
    
    #######################################################
    #### predicted statistics for document
    
    def set_predicted_statistics(
        self                 : Document,
        predicted_statistics : dict
    ) -> None:
        assert not hasattr(self, '_predicted_statistics')
        assert isinstance(predicted_statistics, dict)
        self._predicted_statistics = predicted_statistics
    
    def get_predicted_statistics(self : Document) -> dict:
        assert isinstance(self._predicted_statistics, dict)
        return self._predicted_statistics
    
    
    #######################################################
    #### output representation of document
    
    def get_output_dict(
        self                     : Document,
        predicted_statistics_key : str
    ) -> dict:
        assert isinstance(predicted_statistics_key, str)
        return {
            self._get_id_key_to_use()                    : self.get_id(),
            Document._get_len_in_sents_key()             : self.get_len_in_sents(),
            Document._get_len_in_words_key()             : self.get_len_in_words(),
            Document._get_len_in_chars_key()             : self.get_len_in_chars(),
            Document._get_begin_doc_batch_datetime_key() : self._begin_doc_batch_datetime_to_str(),
            Document._get_end_doc_batch_datetime_key()   : self._end_doc_batch_datetime_to_str(),
            predicted_statistics_key                     : self.get_predicted_statistics()
        }
    
    
    #######################################################
    #### logger
    
    def _set_logger(
        self   : Document,
        logger : Logger
    ) -> None:
        assert hasattr(self, '_logger') is False
        assert isinstance(logger, Logger)
        self._logger = logger
        
    def _get_logger(self : Logger) -> Logger:
        assert isinstance(self._logger, Logger)
        return self._logger
    
    
    #######################################################
    #### enforced maximum sentence length
    
    def _set_max_sent_len_in_chars(
        self                  : Document,
        max_sent_len_in_chars : int
    ) -> None:
        assert hasattr(self, '_max_sent_len_in_chars') is False
        assert isinstance(max_sent_len_in_chars, int)
        assert 0 < max_sent_len_in_chars
        self._max_sent_len_in_chars = max_sent_len_in_chars

    def _get_max_sent_len_in_chars(self : Document) -> int:
        assert 0 < self._max_sent_len_in_chars
        return self._max_sent_len_in_chars
    
    
    #######################################################
    #### constructor, which should not be called directly;
    ####     use the static methods further below to
    ####     initialize Document objects
    
    def __init__(
        self     : Document,
        logger   : Logger,
        doc_dict : dict
    ) -> Document:
        self._set_logger(logger)            ; del logger
        
        doc_id = None
        for id_key in Document._get_id_keys():
            if id_key in doc_dict:
                doc_id = doc_dict[id_key]
                break
        
        if doc_id is None:
            self._get_logger().critical(
                dedent(
                    '''\
                    document.document: Document.__init__:
                    expected to find a doc id json key
                    in the passed doc dict but none was present:
                    the list of possible doc id json keys is {0}
                    and the list of available json keys
                    in the doc dict is {1}'''
                ).replace('\n', ' ').format(
                    Document._get_id_keys(),
                    doc_dict.keys()
                )
            )
            sys.exit(-1)
        
        self._set_id(doc_id)           ; del doc_id
        self._set_id_key_to_use(id_key); del id_key
    
    
    @staticmethod
    def from_input_doc_dict(
        logger                : Logger,
        input_doc_dict        : dict,
        max_sent_len_in_chars : int
    ) -> Document:
        assert isinstance(input_doc_dict, dict)
        
        input_doc = Document(
            logger   = logger,
            doc_dict = input_doc_dict 
        )                                     ; del logger
        input_doc._set_max_sent_len_in_chars(
            max_sent_len_in_chars
        )                                     ; del max_sent_len_in_chars
        
        
        text = None
        for text_key in Document._get_text_keys():
            if text_key in input_doc_dict:
                text = input_doc_dict[text_key]
                break

        if text is None:
            input_doc._get_logger().critical(
                dedent(
                    '''\
                    document.document: Document.from_input_doc_dict:
                    expected to find a doc text json key in the passed
                    doc dict but none was present:
                    the list of possible doc text json keys is {0}
                    and the list of available json keys in the doc
                    dict is {1}'''
                ).replace('\n', ' ').format(
                    Document._get_text_keys(),
                    input_doc_dict.keys()
                )
            )
            sys.exit(-1)
        
        input_doc._set_full_text(text); del text
        
        try:
            nltk.data.find('tokenizers/punkt')
        except LookupError:
            nltk.download('punkt')
        
        list_of_sents = nltk.sent_tokenize(input_doc.get_full_text())
        
        for sent_index in range(len(list_of_sents)):
            if (
                input_doc._get_max_sent_len_in_chars()
                < len(list_of_sents[sent_index])
            ):
                input_doc._get_logger().debug(
                    dedent(
                        '''\
                        document.document: Document.from_input_doc_dict:
                        the sent at index {0}
                        in the doc with id of {1}
                        was truncated from its original length of {2} chars
                        to {3} chars:
                        the truncation threshold can be adjusted with the option
                        "--max-sent-len-in-chars len"'''
                    ).replace('\n', ' ').format(
                        sent_index,
                        input_doc.get_id(),
                        len(list_of_sents[sent_index]),
                        input_doc._get_max_sent_len_in_chars()
                    )
                )

                list_of_sents[sent_index] = \
                    list_of_sents[sent_index][:input_doc._get_max_sent_len_in_chars()]
        
        input_doc._set_list_of_sents(list_of_sents); del list_of_sents
        input_doc._set_len_in_sents(len(input_doc.get_list_of_sents()))
        input_doc._set_len_in_words(
            sum(
                [len(sent.split()) for sent in input_doc.get_list_of_sents()]
            )
        )
        input_doc._set_len_in_chars(
            sum(
                [len(sent) for sent in input_doc.get_list_of_sents()]
            )
        )
        
        return input_doc
        
        
    @staticmethod
    def from_output_doc_dict(
        logger                   : Logger,
        predicted_statistics_key : str,
        output_doc_dict          : dict
    ) -> Document:
        assert isinstance(output_doc_dict, dict)
        
        output_doc = Document(
            logger   = logger,
            doc_dict = output_doc_dict
        )                              ; del logger
        
        #########################################
        ## recognize document length statistics

        output_doc._set_len_in_sents(
            output_doc_dict[
                Document._get_len_in_sents_key()
            ]
        )
        output_doc._set_len_in_words(
            output_doc_dict[
                Document._get_len_in_words_key()
            ]
        )
        output_doc._set_len_in_chars(
            output_doc_dict[
                Document._get_len_in_chars_key()
            ]
        )
        
        #########################################
        ## recognize document batch start and
        ##     end datetimes
        
        begin = output_doc_dict[
            Document._get_begin_doc_batch_datetime_key()
        ]
        output_doc.set_begin_doc_batch_datetime(
            begin if 'not first doc in doc batch' == begin else
            datetime.datetime.fromisoformat(begin)
        )
        
        end = output_doc_dict[
            Document._get_end_doc_batch_datetime_key()
        ]
        output_doc.set_end_doc_batch_datetime(
            end if 'not last doc in doc batch' == end else
            datetime.datetime.fromisoformat(end)
        )
        
        ##############################################
        ## recognize the predicted statistics dict
        
        assert predicted_statistics_key in output_doc_dict, \
            '\n' + \
            f'predicted_statistics_key: {predicted_statistics_key}\n' + \
            f'output_doc_dict.keys()  : {output_doc_dict.keys()}'
        
        output_doc.set_predicted_statistics(
            output_doc_dict[
                predicted_statistics_key
            ]
        )
        
        return output_doc
