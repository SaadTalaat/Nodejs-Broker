Promise = require "promise"
class Broker

    constructor: (name, retry_limit) ->
      @retrial_limit = retry_limit
      @name = name
      @transaction_bucket = {
          'insert':{},
          'insert_retry':{},
          'insert_rank':{},
          'delete':{},
          'delete_retry':{},
          'delete_rank':{},
          'update':{},
          'update_retry':{},
          'update_rank':{},
          'read':{},
          'read_retry':{},
          'read_rank':{},
          'create':{},
          'create_retry':{},
          'create_rank':{}
      }

    __cleanup:(op_label, op_key) ->
        delete @transaction_bucket[op_label][op_key]
        delete @transaction_bucket[op_label + '_rank'][op_key]
        delete @transaction_bucket[op_label + '_retry'][op_key]

    execute:() ->
        if arguments.length < 3
            throw new Error("3 paramaters at least should be passed broker operation (operation, label, key)")
        op_transaction = arguments[0]
        op_label = arguments[1]
        key = arguments[2]
        value = arguments[3]
        op_rank = op_label+"_rank"
        op_retry = op_label+"_retry"
        if @transaction_bucket[op_label][key]
            @transaction_bucket[op_rank][key] +=1
            return @transaction_bucket[op_label][key]
        self = @
        console.log "Issuing #{op_label} with key #{key}"
        deferred_read = new Promise((fulfill, reject) ->
            transaction = new op_transaction(key, value)
            deferred_transaction = transaction.commit()
            deferred_transaction.then(
                (result) ->
                    self.__cleanup(op_label, key)
                    fulfill result
            )
            deferred_transaction.catch(
                (error) ->
                    if self.transaction_bucket[op_retry][key] <= self.retrial_limit and self.transaction_bucket[op_rank][key] > self.transaction_bucket[op_retry][key]
                        oldTrans = self.transaction_bucket[op_label][key]
                        delete self.transaction_bucket[op_label][key]
                        newTrans = self.execute(op_transaction, op_label, key, arguments[3])
                        self.transaction_bucket[op_retry][key] +=1
                        fulfill newTrans
                    else
                        self.__cleanup(op_label, key)
                        reject error
            )
        )

        @transaction_bucket[op_label][key] = deferred_read
        if not @transaction_bucket[op_rank][key]
            @transaction_bucket[op_rank][key] = 0
        if not @transaction_bucket[op_retry][key]
            @transaction_bucket[op_retry][key] = 0
        else
            @transaction_bucket[op_retry][key] +=1
        return deferred_read


module.exports = Broker
