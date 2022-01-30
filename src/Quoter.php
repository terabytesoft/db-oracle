<?php

declare(strict_types=1);

namespace Yiisoft\Db\Oracle;

use PDO;
use Yiisoft\Db\Driver\PDODriver;
use Yiisoft\Db\Schema\Quoter as BaseQuoter;
use Yiisoft\Db\Schema\QuoterInterface;

final class Quoter extends BaseQuoter implements QuoterInterface
{
    public function __construct(
        private string $columnQuoteCharacter,
        private string $tableQuoteCharacter,
        private PDODriver $PDODriver,
        private string $tablePrefix = ''
    ) {
        parent::__construct($columnQuoteCharacter, $tableQuoteCharacter, $PDODriver, $tablePrefix);
    }

    public function quoteValue(int|string $value): int|string
    {
        if (!is_string($value)) {
            return $value;
        }

        /** the driver doesn't support quote (e.g. oci) */
        return "'" . addcslashes(str_replace("'", "''", $value), "\000\n\r\\\032") . "'";
    }
}
