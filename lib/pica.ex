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
  defmacrop return_err(result, whenOkCase) do
    errCase = {:'->', [], [ [{:error, {:reason, [], nil}}], 
                             {:error, {:reason, [], nil}} ]}
    
    withErrCase = case L.keyfind(:do, 1, whenOkCase) do
      {:do, other = [{:'->', _, _} | _]} -> [errCase | other]
      {:do, other} -> [errCase, {:'->',[],[ [{:_,[],nil}], other ]}]
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
  def append( pica_rec( block: b, data: d, bOff: bof, dOff: dof, file: fd ) = pica, data) 
  when d < @maxKey and b < @maxKey do
    nextOff = dof + byte_size(data)
    
    dataWriter = {bof + dof, data}
    posWriter  = {calc_offset_pos(bof, d), pack_offset(nextOff)}
    
    return_err F.pwrite(fd, [dataWriter, posWriter]), 
      do: pica_rec(pica, [ data: d+1, dOff: nextOff ]) |> fin_append
  end
  
  def append(_pica,_data), 
    do: {:error, :eof}
  
  ## append next block
  defp fin_append(pica_rec( block: b, data: @maxKey, bOff: bof, dOff: dof, file: fd ) = pica) 
  when b < @maxKey do
    nextBlock = b + 1
    nextOffset = bof + dof
    
    blockWriter = {calc_offset_pos(0, b), pack_offset(nextOffset)}
    headerSet = {nextOffset + @fullHeader, [0]}
    
    return_err F.pwrite(fd, [headerSet, blockWriter]), 
      do: {:ok, pica_rec(pica, [ block: nextBlock, 
                                 data: 0, 
                                 bOff: nextOffset, 
                                 dOff: @fullHeader ])}
  end
  
  ## normal or pk overflow
  defp fin_append(pica), 
    do: {:ok, pica}
  
  defp pack_offset(n), 
    do: pack_offset(calc_off_crc(n), n)
  defp pack_offset(crc, n), 
    do: <<crc:: @crcBit, n:: @offBit, 0:: @offsetBit>>
  
  
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
    [{location, length}]
  """
  def get_location(pica_rec( file: fd ), pk), 
    do: get_location(fd, pk)
  def get_location(fd, pk) when is_integer(pk), 
    do: get_location(fd, [pk])
  def get_location(fd, {from, to}), 
    do: get_location(fd, [{from, to}])
  def get_location(fd, pkList) do
    return_err do_get_location(fd, pkList),
      do: ( {:ok, loc} -> {:ok, L.flatten(loc)} )
  end
  
  defp do_get_location(fd, pkList) do
    return_err read_address(fd, pkList), 
      do: ( {:ok, addr} -> {:ok, calc_location(addr, [])} )
  end
  
  
  
  ## [{block, bOff, [{data, dOff}]}] -> [ [{position, length}] ]
  ## list of position and length will be grouped by the block
  defp calc_location([], result), 
    do: L.reverse(result)
  defp calc_location([{_blc, offset, dList} | tail], result) do
    faList = start_calc_in_block_location(dList, offset)
    calc_location(tail, [faList | result])
  end
  
  defp start_calc_in_block_location([], _off), 
    do: []
  defp start_calc_in_block_location([{_p, off} | tail], offset), 
    do: calc_in_block_location(tail, offset, [{off + offset, nil}])
    
  defp calc_in_block_location([], _off, [_ | result]), 
    do: L.reverse(result)
  defp calc_in_block_location([{_p, off} | tail], offset, [{dof, nil} | result]) do
    nextOff = off + offset
    calc_in_block_location(tail, offset, [{nextOff, nil}, {dof, nextOff - dof} | result])
  end
  
  
  
  @doc"""
    pica | IoDevice
    pk | {pk, pk} | [pk | {pk, pk}]
    ->
    {:ok, [data]} | {:error, :eof} | {:error, reason}
  """
  def get(pica_rec( file: fd ), pk), 
    do: get(fd, pk)
  def get(fd, {from, to}) do
    return_err do_get_location(fd, [{from, to}]), 
      do: ( {:ok, loc} ->  serial_read(fd, loc) )
  end
  def get(fd, pk) do
    return_err get_location(fd, pk), 
      do: ( {:ok, loc} -> F.pread(fd, loc) )
  end
  
  defp serial_read(fd, loc) do
    return_err ( calc_file_offset(loc, []) |> read_file(fd, []) ), 
      do: ( {:ok, data} -> {:ok, L.flatten(data)} )
  end
  
  ## [ [{position, length}] ] -> [{position, groupLength, [dataLength]}]
  ## for F.pread and bin_split functions
  defp calc_file_offset([faList | tail], result) do
    [{pos, _len} | _] = faList
    {endPos, lenList} = calc_in_block_offset(faList, [])
     calc_file_offset(tail, [{pos, endPos - pos, lenList} | result])
  end
  defp calc_file_offset([], result), do: L.reverse(result)
  
  defp calc_in_block_offset([{pos, len} | []], lenList), 
    do: {pos + len, [len | lenList] |> L.reverse}
  defp calc_in_block_offset([{_pos, len} | tail], lenList), 
    do: calc_in_block_offset(tail, [len | lenList])
  
  
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
    <<data::[binary, size(len)], tail:: binary>> = bin
    split_bin(next, tail, [data|result])
  end
  defp split_bin([], _, result), do: L.reverse(result)
  
  
  @doc """
    open a writing descriptor in last state
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
    return_err F.pwrite(fd, [ {0, <<@version:: @versionBit, @idFix, 0:: @fhBit>>}, 
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
  
  defp check_append_file(<<@version:: @versionBit, @idFix, _opts::binary>>, fd) do
    return_err get_last(fd) do
      {:ok, {{lastBlock, bOff}, {lastData, dOff}}} ->
        pica_rec( block: lastBlock, 
                  data: lastData, 
                  bOff: bOff, 
                  dOff: dOff, 
                  file: fd ) |> fin_append
    end
  end
  
  defp check_append_file(_,_),
    do: {:error, :undefined}
  
  
  
  @doc"""
    pica | IoDevice
    ->
    {:ok, {{blockKey, blockOffset}, {dataKey, dataOffset}}} | {:error, reason}
  """
  def get_last(pica_rec( file: fd )), do: get_last(fd)
  def get_last(fd) do
    return_err read_offset(fd, [{0, 0, @maxKey}]) do
      {:ok, [blockList]} -> 
        L.last(blockList) |> get_last_data_offset(fd)
    end
  end
  
  defp get_last_data_offset({_, offset} = blc, fd) do
    return_err read_offset(fd, [{offset, 0, @maxKey}]), 
      do: ( {:ok, [dataOffList]} -> {:ok, {blc, L.last(dataOffList)}} )
  end
  
  
  @doc"""
    read real offset value from disk
    
    for minimize reverse function call,
    most of internal list processing functions return reversed result
    variable name with r prefix is mark of reversed condition
    
    pica | IoDevice
    [pk | {pk, pk}]
    ->
    [{blockKey, blockOffset, [{dataKey, dataOffset}]}] | {:error, :eof} | {:error, reason}
  """
  def read_address(pica_rec( file: fd ), keyList), 
    do: read_address(fd, keyList)
  def read_address(fd, keyList) do
    return_err (try do
      set_key_list(keyList, [])
    catch
      _,_ -> {:error, :badarg}
    end) do 
      rKeyList ->
        return_err read_block_offset(fd, rKeyList) do
          {:ok, rBlockOffList} -> read_data_offset(fd, rBlockOffList, rKeyList)
        end
    end
  end
  
  ## reversed result
  defp set_key_list([{from,to} | tail], result) when from < @maxPk and to < @maxPk and from <= to,
    do: set_key_list(tail, [get_read_range(from, to) | result])
  defp set_key_list([key | tail], result) when key < @maxPk,
    do: set_key_list(tail, [get_read_range(key, key) | result])
  defp set_key_list([], result), do: L.flatten(result)
  
  ## pk ~ ok -> logical address of block and data
  ## reversed result
  defp get_read_range(from, to), 
    do: calc_read_range( to_subk(from), to_subk(to), [] )
  
  ## reversed result
  @maxRange @maxKey - 1
  defp calc_read_range({hh, ht}, {hh, tt}, result), do: [{hh, ht, tt} | result]
  defp calc_read_range({hh, ht}, {th, tt}, result), 
    do: calc_read_range({hh + 1, 0}, {th, tt}, [{hh, ht, @maxRange} | result])
  
  
  defp read_block_offset(fd, rKeyList) do
    blockList = set_read_block_offset(rKeyList, [])
    return_err read_offset(fd, blockList) do
      {:ok, blockOffsetList} ->
        set_result_block_offset(blockOffsetList, [])
    end
  end
  
  ## reversed result
  defp set_read_block_offset([{b, _, _} | tail], result), 
    do: set_read_block_offset(tail, [{0, b, b} | result])
  defp set_read_block_offset([], result), do: result
  
  ## reversed result
  defp set_result_block_offset([[bOff | _] | tail], result),
    do: set_result_block_offset(tail, [bOff | result])
  defp set_result_block_offset([[] | _t], _r), 
    do: {:error, :eof}
  defp set_result_block_offset([], result), do: {:ok, result}
  
  
  ## reversed result
  defp read_data_offset(fd, rBlockOffList, rKeyList) do
    dataList = set_read_data_offset(rBlockOffList, rKeyList, [])
    return_err read_offset(fd, dataList) do
      {:ok, dataOffsetList} -> 
        set_result_data_offset(rBlockOffList, dataOffsetList |> L.reverse, [])
    end
  end
  
  ## reversed result
  defp set_read_data_offset([{_, off} | bTail], [{_, from, to} | dTail], result), 
    do: set_read_data_offset(bTail, dTail, [{off, from, to} | result])
  defp set_read_data_offset([], [], result), do: result
  
  ## reversed result
  defp set_result_data_offset([{block, off} | bTail], [data | dTail], result) do
    case data do
      [[] | _] -> {:error, :eof}
      [_ | []] -> {:error, :eof}
      _ok -> set_result_data_offset(bTail, dTail, [ {block, off, data} | result ])
    end
  end
  defp set_result_data_offset([], [], result), do: {:ok, result}
  
  
  ## IoDevice
  ## reqList :: [[{offset, from, to}]]
  ## ->
  ## {:ok, [[{subKey, offset}]]} | {:error, :eof} | {:error, reason}
  defp read_offset(fd, reqList) do
    posList = calc_raw_offset_pos(reqList, [])
    return_err F.pread(fd, posList) do
       :eof -> {:error, :eof}
      {:ok, binList} -> 
        {:ok, check_and_list_raw_offset(binList, reqList)}
    end
  end
  
  defp calc_raw_offset_pos([{offset, from, to} | tail], result) do
    from  = if from == 0 do 0 else from - 1 end
    start = calc_offset_pos(offset, from)
    fin   = calc_offset_pos(offset, to)
    calc_raw_offset_pos(tail, [{start, fin - start + @offsetByte} | result])
  end
  defp calc_raw_offset_pos([], result), do: L.reverse(result)
  
  defp check_and_list_raw_offset(binList, reqList) do
    fn (bin, {_o, from, _}) -> 
      revise_offset_binary(bin, from) 
      |> check_and_build_offset_list(from, [])
    end |> L.zipwith(binList, reqList)
  end
  
  ## insert 0 offset when requested 0
  defp revise_offset_binary(bin, 0) do 
    crc  = Erlang.crc32( <<@fullHeader :: @offBit>> )
    off0 = <<crc:: @crcBit, @fullHeader:: @offBit>>
    <<off0:: binary, bin:: binary>>
  end
  defp revise_offset_binary(bin, _), do: bin
  
  ## crc check and list build
  defp check_and_build_offset_list(<<crc:: @crcBit, off:: @offBit, tail:: binary>>, subKey, result) do
    case calc_off_crc(off) do
      ^crc -> check_and_build_offset_list(tail, subKey + 1, [{subKey, off} | result])
      _any -> L.reverse(result)
    end
  end
  defp check_and_build_offset_list(_, _, result), do: L.reverse(result)
  
  
  
  @doc"""
    get last inserted pk
    
    pica
    ->
    pk
  """
  def last_pk(pica), 
    do: current_pk(pica) - 1
    
  @doc"""
    get current inserting pk
    
    pica
    ->
    pk
  """
  def current_pk(pica_rec( block: b, data: d )), 
    do: to_pk(b, d)
  
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


# """

defmodule Pica.Test do
  
  alias :erlang,  as: Erlang
  alias :math,    as: Math
  
  @subKeyBit   18
  @keyBit      @subKeyBit * 2
  @maxKey      Erlang.trunc(Math.pow(2, @subKeyBit))
  @maxPk       @maxKey * @maxKey
  
  @mReadV      1000
  
  def start(name, n) do
    {:ok, pica} = Pica.open(name)
    startNum = Pica.last_pk(pica)
    start = Pica.current_pk(pica)
    
    n = if (start + n) < @maxPk do n else @maxPk - start end
    
    :io.format 'test start from ~p ~n', [start]
    
    {t1, {:ok, pica}} = :timer.tc fn() ->
      do_loop_append_test(pica, name, :crypto.strong_rand_bytes(2048), start, n, 0)
    end
    :io.format 'append loop end time>> ~p~n~n', [t1]
    
    
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
  
  defp do_loop_append_test(pica,_name,_bin,_start, 0,_v), do: {:ok, pica}
  defp do_loop_append_test(pica, name, bin, start, n, v) do
    data = {v, :binary.part( bin, 0, :crypto.rand_uniform(1, 1024) )}
    dPack = Erlang.term_to_binary(data)
    crc = Erlang.crc32 dPack
    {:ok, pica} = Pica.append pica, <<crc::32, dPack::binary>>
    if rem(n, 4500) == 0 do
      Pica.close(pica)
      {:ok, pica} = Pica.open(name)
      
      do_loop_append_test(pica, name, bin, start, n-1, v+1)
    else
      do_loop_append_test(pica, name, bin, start, n-1, v+1)
    end
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

# """