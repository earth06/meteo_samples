####自分で設定するパラメータ
#ファイル名
log_filename="zokusei_feature.log" 
#ログのフォーマット 詳細は
#https://srbrnote.work/archives/4472
_detail_formatting = '%(asctime)s %(levelname)-8s [%(module)s#%(funcName)s %(lineno)d] %(message)s'
####loggerの設定
from logging import getLogger, Formatter, StreamHandler, FileHandler, DEBUG
logger = getLogger(__name__)
#handlerの設定(標準出力用)
s_handler = StreamHandler()
s_handler.setFormatter(Formatter(_detail_formatting))
s_handler.setLevel(DEBUG)
#handlerの設定(ファイル出力用)
f_handler = FileHandler(log_filename)
f_handler.setFormatter(Formatter(_detail_formatting))
f_handler.setLevel(DEBUG)
logger.setLevel(DEBUG)
logger.addHandler(s_handler)
logger.addHandler(f_handler)
logger.propagate = False
#loggerの設定おわり 以下print()のかわりにlogger.info()を使う
logger.info("ここに書いた文字が時刻とかといっしょに記録される")