<?php

declare(strict_types=1);

namespace Yiisoft\Db\Oracle;

use Yiisoft\Db\Command\DDLCommand as AbstractDDLCommand;
use Yiisoft\Db\Schema\QuoterInterface;

final class DDLCommand extends AbstractDDLCommand
{
    public function __construct(private QuoterInterface $quoter)
    {
        parent::__construct($quoter);
    }

    public function dropCommentFromColumn(string $table, string $column): string
    {
        return 'COMMENT ON COLUMN ' . $this->quoter->quoteTableName($table) . '.' . $this->quoter->quoteColumnName($column)
            . " IS ''";
    }

    public function dropCommentFromTable(string $table): string
    {
        return 'COMMENT ON TABLE ' . $this->quoter->quoteTableName($table) . " IS ''";
    }
}
