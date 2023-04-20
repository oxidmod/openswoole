<?php

declare(strict_types=1);
/**
 * This file is part of OpenSwoole.
 * @link     https://openswoole.com
 * @contact  hello@openswoole.com
 */
namespace OpenSwoole\GRPC;

use OpenSwoole\GRPC\Exception\GRPCException;

class BaseStub
{
    private $client;

    private $deserialize;

    private $streamId;

    public function __construct($client)
    {
        if ($client) {
            $this->client = $client;
        }
    }

    protected function _simpleRequest(
        $method,
        $request,
        $deserialize,
        array $metadata = [],
        $timeout = -1
    ) {
        $streamId              = $this->client->send($method, $request);
        [$data, $trailers]     = $this->client->recv($streamId, $timeout);

        if ($trailers['grpc-status'] !== '0') {
            throw new GRPCException($trailers['grpc-message']);
        }
        return $this->_deserializeResponse($deserialize, $data);
    }

    protected function _deserializeResponse($deserialize, $value)
    {
        if ($value === null) {
            return;
        }

        [$className, $deserializeFunc] = $deserialize;
        $obj                           = new $className();
        $obj->mergeFromString($value);
        return $obj;
    }

    protected function _serverStreamRequest(
        $method,
        $request,
        $deserialize,
        array $metadata = [],
        $timeout = -1
    ) {
        $this->deserialize = $deserialize;
        $streamId          = $this->client->send($method, $request);
        [$data]            = $this->client->recv($streamId, $timeout);

        $this->streamId    = $streamId;
        return $this->_deserializeResponse($deserialize, $data);
    }

    protected function _getData($timeout = -1)
    {
        [$data]     = $this->client->recv($this->streamId, $timeout);

        return $this->_deserializeResponse($this->deserialize, $data);
    }
}
