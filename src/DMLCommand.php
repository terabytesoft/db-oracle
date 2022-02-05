<?php

declare(strict_types=1);

namespace Yiisoft\Db\Oracle;

use InvalidArgumentException;
use Throwable;
use Yiisoft\Db\Command\DMLCommand as AbstractDMLCommand;
use Yiisoft\Db\Exception\Exception;
use Yiisoft\Db\Exception\InvalidConfigException;
use Yiisoft\Db\Schema\QuoterInterface;

final class DMLCommand extends AbstractDMLCommand
{
    public function __construct(private QuoterInterface $quoter)
    {
        parent::__construct($quoter);
    }
}
