defmodule Pica do

  alias :erlang,  as: Erlang
  alias :file,    as: F
  alias :filelib, as: Flib
  alias :lists,   as: L
  alias :math,    as: Math
  
  require Record
  
  ## set your own version
  @version     1
  @versionByte 2
  @versionBit  @versionByte * 8
  @idFix       "pica"
  @fixByte     byte_size(@idFix) + @versionByte
  @fixBit      @fixByte * 8
  
  ## /version(2), pica(4), 0(2), 0(24)
  @header      32
  @headerFill  (@header - @fixByte) * 8
  
  ## crc32 + 48Bit position
  @crcByte     4
  @offByte     6
  @offsetByte  @crcByte + @offByte
  @footer      @offsetByte
  
  @crcBit      @crcByte * 8
  @offBit      @offByte * 8
  @offsetBit   @offsetByte * 8
  
  ## by practical reason, limitation of subKeyBit will be sub 20
  @subKeyBit   18
  @keyBit      @subKeyBit * 2
  @maxKey      Erlang.trunc(Math.pow(2, @subKeyBit))
  @maxPk       @maxKey * @maxKey
  
  @offsetTable @offsetByte * @maxKey
  @fullHeader  @offsetTable + @header + @footer
  @fhBit       @fullHeader * 8
  
  Record.defrecordp :pica_rec,
                    [ block: 0,
                      data: 0,
                      bOff: 0,
                      dOff: 0,
                      file: nil ]
  
  
  
  ## macro
  defmacrop return_err(result, doBlock) do
    errCase = {:'->', [], [ [{:error, {:reason, [], nil}}], 
                             {:error, {:reason, [], nil}} ]}
    
    withErrCase = case L.keyfind(:do, 1, doBlock) do
                    {:do, ok = [{:'->', _, _} | _]} -> [errCase | ok]
                    {:do, ok} -> [errCase, {:'->',[],[ [{:_,[],nil}], ok ]}]
                  end
    
    quote do: ( case unquote(result), do: unquote(withErrCase) )
  end
  
  @doc"""
    close file
    
    pica :: pica_rec
    ->
    :ok | {:error, reason}
  """
  def close(pica_rec( file: fd )), 
    do: F.close(fd)
  
  
  @doc"""
    append data to pica file
    
    pica
    data :: binary
    ->
    {:ok, pica} | {:error, :eof} | {:error, reason}
  """
  
  def append( pica_rec( block: b, data: d, bOff: bof, dOff: dof, file: fd ) = pica, data ) when is_list(data) do
    return_err build_append_data({b, d, bof, dof}, data, [], []) do
      {{b, d, bof, dof}, offList, dataList} ->
        return_err F.pwrite(fd, L.flatten([dataList, offList])), 
          do: {:ok, pica_rec( pica, [ block: b, data: d, bOff: bof, dOff: dof ])}
    end
  end
  def append(pica = pica_rec(), data),
    do: append(pica, [data])
  
  
  
  defp build_append_data({b, d, bof, dof}, [data | tail], ol, dl) 
  when d < @maxKey and b < @maxKey do
    nextOff    =  dof + byte_size(data)
    dataWriter = {bof + dof, data}
    offWriter  = {calc_offset_pos(bof, d), pack_offset(nextOff)}
    
    {b, d+1, bof, nextOff}
    |> build_append_data(tail, [offWriter | ol], [dataWriter | dl])
  end
  
  
  ## append next block
  defp build_append_data({b, @maxKey, bof, dof}, data, ol, dl) 
  when b < @maxKey do
    nextOff     =  bof + dof
    blockWriter = {calc_offset_pos(0, b), pack_offset(nextOff)}
    
    nextOl = case data do
               [] -> [{nextOff + @fullHeader, <<0>>}, blockWriter | ol]
               _o -> [blockWriter | ol]
             end
    
    {b + 1, 0, nextOff, @fullHeader}
    |> build_append_data(data, nextOl, dl)
  end
  
  defp build_append_data(state, [], offList, dataList), 
    do: {state, pack_append_data(offList), pack_append_data(dataList)}
  
  defp build_append_data(_p, _d, _ol, _dl), do: {:error, :eof}
  
  defp pack_append_data([{pos, data} | tail]), do: pack_append_data(tail, [{pos, [data]}])
  defp pack_append_data([]), do: []
  
  defp pack_append_data([{pos, data} | tail], result = [{lastPos, resultList} | resultTail]) do
    case lastPos - byte_size(data) do
      ^pos -> 
        pack_append_data(tail, [{pos, [data | resultList]} | resultTail])
      _any ->
        pack_append_data(tail, [{pos, [data]} | result])
    end
  end
  defp pack_append_data([], result), do: result
  
  
  
  
  defp pack_offset(n), 
    do: pack_offset(calc_off_crc(n), n)
  defp pack_offset(crc, n), 
    do: <<crc :: @crcBit, n :: @offBit>>
  
  @doc"""
    set the pica record to the past position
    this is part of rollback method idea
    but, rollback function is not yet developed
    
    so, do not use this function for critical issue
    
    pica
    pk :: integer
    ->
    {:ok, pica} | {:error, :eof} | {:error, reason}
  """
  def past_to(pica_rec( file: fd ), pk) do
    return_err read_address(fd, [pk]) do
      {:ok, [{b, offset, [{d, start}, {_,_fin}]}]} ->
        ^pk = to_pk(b,d)
        {h, t} = to_subk(pk)
        {:ok, pica_rec( block: h, 
                        data: t, 
                        bOff: offset, 
                        dOff: start, 
                        file: fd )}
      _other -> {:error, :eof}
    end
  end
  
  
  
  @doc"""
    pica | IoDevice :: F.IoDevice
    pk | {pk, pk} | [pk | {pk, pk}]
    ->
    {:ok, [{location, length}]} | {:error, :eof} | {:error, :reason}
  """
  def get_location(pi, pk) when is_integer(pk), 
    do: get_location(pi, [pk])
  def get_location(pi, {from, to}), 
    do: get_location(pi, [{from, to}])
  def get_location(pi, pkList) do
    return_err do_get_location(pi, pkList),
      do: ( {:ok, loc} -> {:ok, L.flatten(loc)} )
  end
  
  defp do_get_location(pi, pkList) do
    return_err read_address(pi, pkList), 
      do: ( {:ok, addr} -> {:ok, calc_location(addr)} )
  end
  
  
  ## [{block, bOff, [{data, dOff}]}] -> [ [{position, length}] ]
  ## list of position and length will be grouped by the block
  defp calc_location(addr) do
    fn({_b, offset, dList}) ->
      case dList do
        [] -> []
        [{_p, off} | tail] ->
          calc_location(tail, offset, [{off + offset, nil}])
      end
    end |> L.map(addr)
  end
  
  defp calc_location([], _off, [_ | result]), 
    do: L.reverse(result)
  defp calc_location([{_p, off} | tail], offset, [{dof, nil} | result]) do
    nextOff = off + offset
    calc_location(tail, offset, [{nextOff, nil}, {dof, nextOff - dof} | result])
  end
  
  
  
  @doc"""
    pica | IoDevice
    pk | {pk, pk} | [pk | {pk, pk}]
    ->
    {:ok, [data]} | {:error, :eof} | {:error, reason}
  """
  def get(pi = pica_rec( file: fd ), {from, to}) do
    return_err do_get_location(pi, [{from, to}]), 
      do: ( {:ok, loc} -> serial_read(fd, loc) )
  end
  def get(pi = pica_rec( file: fd ), pk) do
    return_err get_location(pi, pk), 
      do: ( {:ok, loc} -> F.pread(fd, loc) )
  end
  
  defp serial_read(fd, loc) do
    return_err ( calc_file_offset(loc) |> read_file(fd, []) ), 
      do: ( {:ok, data} -> {:ok, L.flatten(data)} )
  end
  
  ## [ [{position, length}] ] -> [{position, groupLength, [dataLength]}]
  ## for F.pread and bin_split functions
  defp calc_file_offset(loc) do
    fn([first | _] = faList) ->
      {sPos, _sLen} = first
      {lenList, {ePos, eLen}} = calc_in_block_offset(faList, first)
      {sPos, ePos + eLen - sPos, lenList}
    end |> L.map(loc)
  end
  
  defp calc_in_block_offset(faList, first) do
    fn(last = {_pos, len}, _) -> {len, last}
    end |> L.mapfoldl(first, faList)
  end
  
  defp read_file([], _fd, []), 
    do: {:error, :eof}
  defp read_file([], _fd, result), 
    do: {:ok, L.reverse(result)}
  defp read_file([{start, len, lenList} | tail], fd, result) do
    return_err F.pread(fd, start, len) do
      {:ok, bin} ->
        dataList = split_bin(lenList, bin, [])
        read_file(tail, fd, [dataList | result])
    end
  end
  
  defp split_bin([len|next], bin, result) do
    <<data :: binary-size(len), tail :: binary>> = bin
    split_bin(next, tail, [data|result])
  end
  defp split_bin([], _, result), do: L.reverse(result)
  
  
  @doc """
    open writing descriptor in last state
    for read working, use normal erlang file descriptor with raw and binary options
    
    path :: F.Filename
    ->
    {:ok, pica} | {:error, :undefined} | {:error, reason}
  """
  def open(path) do
    append_to = Flib.is_file(path)
    return_err F.open(path, [:read, :write, :raw, :binary]) do
      {:ok, fd} -> 
        case do_open(append_to, fd) do
          {:error, reason} -> 
            F.close(fd)
            {:error, reason}
          ok -> ok
        end
    end
  end
  
  defp do_open(true,  fd), do: set_append_file(fd)
  defp do_open(false, fd), do: set_new_file(fd)
  
  defp set_new_file(fd) do
    return_err F.pwrite(fd, [ {0, <<@version :: @versionBit, @idFix, 0 :: @fhBit>>}, 
                              {(@fullHeader * 2) - @fixByte, [0]} ]), 
      do: {:ok, pica_rec([ block: 0, 
                           data: 0, 
                           bOff: @fullHeader, 
                           dOff: @fullHeader, 
                           file: fd])}
  end
  
  defp set_append_file(fd) do
    return_err F.pread(fd, 0, @header) do
       :eof -> {:error, :undefined}
      {:ok, bin} -> check_append_file(bin, fd)
    end
  end
  
  defp check_append_file(<<@version :: @versionBit, @idFix, _opts :: binary>>, fd) do
    return_err get_last(fd) do
      {:ok, {{lastBlock, bOff}, {lastData, dOff}}} ->
        {:ok, pica_rec( block: lastBlock, 
                        data: lastData, 
                        bOff: bOff, 
                        dOff: dOff, 
                        file: fd )}
    end
  end
  
  defp check_append_file(_,_),
    do: {:error, :undefined}
  
  
  
  @doc"""
    pica | IoDevice
    ->
    {:ok, {{blockKey, blockOffset}, {dataKey, dataOffset}}} | {:error, reason}
  """
  def get_last(pi) do
    return_err read_offset(pi, [{0, 0, @maxKey}]) do
      {_, [bOffList]} -> L.last(bOffList) |> get_last_data_offset(pi)
    end
  end
  
  defp get_last_data_offset({_, offset} = blc, pi) do
    return_err read_offset(pi, [{offset, 0, @maxKey}]) do
      {_, [dOffList]} -> {:ok, {blc, L.last(dOffList)}}
    end
  end
  
  
  @doc"""
    read real offset value from disk
    reversed list variable has prefix r
    
    pica | IoDevice
    [pk | {pk, pk}]
    ->
    [{blockKey, blockOffset, [{dataKey, dataOffset}]}] | {:error, :eof} | {:error, reason}
  """
  def read_address(pi, keyList) do
    return_err (try do
      set_key_list(keyList, [])
    catch
      _,_ -> {:error, :badarg}
    end) do 
      rKeyList ->
        return_err read_block_offset(pi, rKeyList) do
          {_, rBlockOffList} -> 
            read_data_offset(pi, rBlockOffList, rKeyList)
        end
    end
  end
  
  ## reversed result
  defp set_key_list([key | kTail], result) do
    case key do
      {from, to} when from < @maxPk and to < @maxPk and from <= to ->
        set_key_list(kTail, [get_read_range(from, to) | result])
      key when key < @maxPk ->
        set_key_list(kTail, [get_read_range(key, key) | result])
    end
  end
  defp set_key_list([], result), do: L.flatten(result)
  
  ## pk ~ ok -> logical address of block and data
  ## reversed result
  defp get_read_range(from, to), 
    do: calc_read_range( to_subk(from), to_subk(to), [] )
  
  ## reversed result
  @maxRange @maxKey - 1
  defp calc_read_range({hh, ht}, {hh, tt}, result), 
    do: [{hh, ht, tt} | result]
  defp calc_read_range({hh, ht}, {th, tt}, result), 
    do: calc_read_range({hh + 1, 0}, {th, tt}, [{hh, ht, @maxRange} | result])
  
  
  defp read_block_offset(pi, rKeyList) do
    rBlockList = fn({b,_,_}) -> {0, b, b} end |> L.map(rKeyList)
    return_err read_offset(pi, rBlockList) do
      {_, rbol} ->
        {:ok, fn (v) -> 
                [bOff | _] = v
                 bOff 
              end |> L.map(rbol)}
    end
  end
  
  
  ## reversed result
  defp read_data_offset(pi, rbol, rKeyList) do
    return_err zip_block_and_key(rbol, rKeyList) do
      rDataList ->
        return_err read_offset(pi, rDataList) do
          {:eof, _} -> {:error, :eof}
          {:ok, rdol} -> 
            set_result_data_offset(rbol, rdol, [])
        end
    end
  end
  
  ## {:error, :eof} when nil block offset condition
  defp zip_block_and_key(rbol, rkl) do
    try do
      fn ({_, off}, {_, from, to}) -> 
        {off, from, to}
      end |> L.zipwith(rbol, rkl)
    catch
      _, _ -> {:error, :eof}
    end
  end
  
  ## reversed result
  defp set_result_data_offset([{block, off} | bTail], [data | dTail], result), 
    do: set_result_data_offset(bTail, dTail, [ {block, off, data} | result ])
  defp set_result_data_offset([], [], result), do: {:ok, result}
  
  
  @doc"""
    return sub key and offset
    
    from <= subKey <= to + 1
    
    pica | IoDevice
    reqList :: [{offset, from, to}]
    ->
    {:ok, [[{subKey :: integer, offset}]]} | {:eof, [[{subKey, offset}]]} | {:error, reason}
  """
  def read_offset(pica_rec(file: fd), reqList), 
    do: read_offset(fd, reqList)
  def read_offset(fd, reqList) do
    posList = calc_raw_offset_pos(reqList, [])
    return_err F.pread(fd, posList) do
       :eof -> {:eof, []} 
      {:ok, binList} -> 
        check_offset_and_eof(binList, reqList, [], :ok)
    end
  end
  
  defp calc_raw_offset_pos([{offset, from, to} | tail], result) do
    from  = if from == 0 do 0 else from - 1 end
    start = calc_offset_pos(offset, from)
    fin   = calc_offset_pos(offset, to)
    calc_raw_offset_pos(tail, [{start, fin - start + @offsetByte} | result])
  end
  defp calc_raw_offset_pos([], result), do: L.reverse(result)
  
  defp check_offset_and_eof([bin | bTail], [{_o, from, _} | rTail], result, state) do
    case revise_offset_binary(bin, from) 
         |> check_and_build_offset_list(from, []) do
      {:eof, []} -> {:eof, L.reverse(result)}
      {:eof,  r} -> check_offset_and_eof(bTail, rTail, [r | result], :eof)
      {:ok,   r} -> check_offset_and_eof(bTail, rTail, [r | result], state)
    end
  end
  defp check_offset_and_eof([], [], result, state), do: {state, L.reverse(result)}
  
  ## insert 0 offset when requested 0
  ## pre calculated crc value
    offBit = @offBit
    @crc32_0 Erlang.crc32( <<@fullHeader :: integer-size(offBit)>> )
  defp revise_offset_binary(bin, 0), 
    do: <<@crc32_0 :: @crcBit, @fullHeader :: @offBit, bin :: binary>>
  defp revise_offset_binary(bin, _), do: bin
  
  ## crc check and list build
  defp check_and_build_offset_list(<<crc:: @crcBit, off:: @offBit, tail:: binary>>, subKey, result) do
    case calc_off_crc(off) do
      ^crc -> check_and_build_offset_list(tail, subKey + 1, [{subKey, off} | result])
      _any -> {:eof, L.reverse(result)}
    end
  end
  defp check_and_build_offset_list(_, _, result), do: {:ok, L.reverse(result)}
  
  
  
  @doc"""
    get last inserted pk
    
    pica
    ->
    pk
    
    or
    
    IoDevice
    ->
    pk | {:error, reason}
  """
  def last_pk(pica) do
    return_err current_pk(pica),
      do: ( cpk -> cpk - 1 )
  end

  
  @doc"""
    get current inserting pk
    raw IoDevice argument need disk reading processes
    use pica argument as possible
    
    pica
    ->
    pk
    
    or
    
    IoDevice
    ->
    pk | {:error, reason}
  """
  def current_pk(pica_rec( block: b, data: d )), 
    do: to_pk(b, d)
  def current_pk(fd) do
    return_err get_last(fd),
      do: ( {:ok, {{b, _}, {d, _}}} -> to_pk(b, d) )
  end
  
  defp to_pk(h, t), do: 
    to_pk(<<h:: @subKeyBit, t:: @subKeyBit>>)
  defp to_pk(<<pk:: @keyBit>>), 
    do: pk
  
  defp to_subk(pk),
    do: do_to_subk(<<pk:: @keyBit>>)
  defp do_to_subk(<<h:: @subKeyBit, t:: @subKeyBit>>),
    do: {h, t}
  
  defp calc_off_crc(n), 
    do: calc_crc(<<n:: @offBit>>)
  defp calc_crc(data),
    do: Erlang.crc32(data)
  
  defp calc_offset_pos(offset, count), 
    do: offset + @header + (count * @offsetByte) 
  
end


"""

defmodule Pica.Test do
  
  alias :erlang,  as: Erlang
  alias :lists,   as: L
  alias :math,    as: Math
  
  @subKeyBit   18
  @keyBit      @subKeyBit * 2
  @maxKey      Erlang.trunc(Math.pow(2, @subKeyBit))
  @maxPk       @maxKey * @maxKey
  
  @mReadV      1000
  
  @restart     5000
  
  def start(name, n, bAppend \\ 1) do
    {:ok, pica} = Pica.open(name)
    startNum = Pica.last_pk(pica)
    start = Pica.current_pk(pica)
    
    n = if (start + n) < @maxPk do n else @maxPk - start end
    
    :io.format 'test start from ~p ~n', [start]
    
    {t1, {:ok, pica}} = :timer.tc fn() ->
      do_loop_append_test(pica, name, :crypto.strong_rand_bytes(2048), start, n, 0, bAppend)
    end
    :io.format 'append loop end time>> ~p~n~n', [t1]
    
    {te, result} = :timer.tc fn() ->
      if n > @mReadV do else nil end
    end
    
    pica = if result != nil do 
      :io.format 'append test 2 end time>> ~p~n~n', [te]
      result 
    else 
      pica 
    end
    
    {t2, {:ok, count1}} = :timer.tc fn() ->
      do_check_test(pica, start, 0, n)
    end
    :io.format 'get loop end time>> ~p~n', [t2]
    :io.format 'get loop end count>> ~p~n~n', [count1]
    
    
    {t3, count2} = :timer.tc fn() ->
      s_read_test(pica, start, n, 0)
    end
    :io.format 'serial get loop end time>> ~p~n', [t3]
    :io.format 'serial get loop end count>> ~p~n~n', [count2]
    
    Pica.close(pica)
    { :ok, {Pica.last_pk(pica) - startNum} }
  end
  
  defp do_loop_append_test(pica,_name,_bin,_start, 0,_v,_ba), do: {:ok, pica}
  defp do_loop_append_test(pica, name, bin, start, n, v, ba) do
  
    now = if (n - ba) > 0 do ba else n end
    {v2, data} = create_append_test_data(v, now, bin, [])
    
    {:ok, pica} = Pica.append(pica, data)
    if rem(n, @restart) == 0 do
      Pica.close(pica)
      {:ok, pica} = Pica.open(name)
      
      do_loop_append_test(pica, name, bin, start, n-now, v2, ba)
    else
      do_loop_append_test(pica, name, bin, start, n-now, v2, ba)
    end
  end
  
  defp create_append_test_data(v, 0,_bin, r), do: {v, L.reverse(r)}
  defp create_append_test_data(v, n, bin, r) do
    data = {v, :binary.part( bin, 0, :crypto.rand_uniform(1, 2048) )}
    # data = {v, :binary.part( bin, 0, 400 )}
    dPack = Erlang.term_to_binary(data)
    crc = Erlang.crc32(dPack)
    create_append_test_data(v + 1, n - 1, bin, [<<crc::32, dPack::binary>> | r])
  end
  
  defp do_check_test(_p, _s, v, v), do: {:ok, v}
  defp do_check_test(pica, start, v, vv) do
    {:ok, [b]} = Pica.get pica, (start + v)
    
    vector = v
    
    {^vector, _bin} = d_check(b)
    do_check_test(pica, start, v+1, vv)
  end
  
  defp s_read_test(_pica, _start, n, n), do: n
  defp s_read_test(pica, start, n, v) do
    from = start + v
    to = if (from + @mReadV) < (start + n) do from + @mReadV else start + n - 1 end
    
    {:ok, result} = Pica.get pica, {from, to}
    
    v = check_s_read_test(result, v)
    s_read_test(pica, start, n, v)
  end
  
  defp check_s_read_test([], v), do: v
  defp check_s_read_test([b|tail], v) do
    vector = v
    {^vector, _bin} = d_check(b)
    check_s_read_test(tail, v+1)
  end
  
  defp d_check(<<crc::32, d::binary>>) do
    ^crc = Erlang.crc32(d)
    Erlang.binary_to_term(d)
  end

end

"""